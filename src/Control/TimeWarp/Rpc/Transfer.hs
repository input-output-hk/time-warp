{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE Rank2Types            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ViewPatterns          #-}

-- |
-- Module      : Control.TimeWarp.Rpc.Transfer
-- Copyright   : (c) Serokell, 2016
-- License     : GPL-3 (see the file LICENSE)
-- Maintainer  : Serokell <hi@serokell.io>
-- Stability   : experimental
-- Portability : POSIX, GHC
--
-- This module provides implementation of `MonadTransfer`.
--
-- It operates with so called /lively sockets/, so that, if error occured while sending
-- or receiving, it would try to restore connection before reporting error.
--
-- When some data is sent for first time to given address, connection with single
-- lively-socket is created; it would be reused for further sends until closed.
--
-- Then server is getting up at some port, it creates single thread to handle incoming
-- connections, then for each input connection lively-socket is created.
--
-- TODO [TW-67]: close all connections upon quiting `Transfer` monad.
--
--
-- About lively sockets:
--
-- Lively socket keeps queue of byte chunks inside.
-- For given lively-socket, @send@ function just pushes chunks to send-queue, whenever
-- @receive@ infinitelly acquires chunks from receive-queue.
-- Those queues are connected to plain socket behind the scene.
--
-- Let's say lively socket to be /active/ if it successfully sends and receives
-- required data at the moment.
-- Upon becoming active, lively socket spawns `processing-observer` thread, which itself
-- spawns 3 threads: one pushes chunks from send-queue to socket, another one
-- pulls chunks from socket to receive-queue, and the last tracks whether socket was
-- closed.
-- Processor thread finishes in one of the following cases:
--
--    * One of it's children threads threw an error
--
--    * Socket was closed
--
-- If some error occures, lively socket goes to exceptional state (which is not expressed
-- in code, however), where it could be closed or provided with newly created plain socket
-- to continue work with and thus become active again.
--
-- UPGRADE-NOTE [TW-59]:
-- Currently, if an error in listener occures (parse error), socket gets closed.
-- Need to make it reconnect, if possible.


module Control.TimeWarp.Rpc.Transfer
       (
       -- * Transfer
         Transfer (..)
       , TransferException (..)
       , runTransfer
       , runTransferS

       -- * Settings
       , FailsInRow
       , Settings (..)
       ) where

import qualified Control.Concurrent                 as C
import           Control.Concurrent.STM             (STM, atomically, check, orElse)
import qualified Control.Concurrent.STM.TBMChan     as TBM
import qualified Control.Concurrent.STM.TChan       as TC
import qualified Control.Concurrent.STM.TVar        as TV
import           Control.Lens                       (at, at, each, makeLenses, use, view,
                                                     (.=), (?=), (^..))
import           Control.Monad                      (forM_, forever, guard, unless, when)
import           Control.Monad.Base                 (MonadBase)
import           Control.Monad.Catch                (Exception, MonadCatch,
                                                     MonadMask (mask), MonadThrow (..),
                                                     bracket, bracketOnError, catchAll,
                                                     finally, handleAll, onException,
                                                     throwM)
import           Control.Monad.Morph                (hoist)
import           Control.Monad.Reader               (ReaderT (..), ask)
import           Control.Monad.State                (StateT (..))
import           Control.Monad.Trans                (MonadIO (..), lift)
import           Control.Monad.Trans.Control        (MonadBaseControl (..))

import           Control.Monad.Extra                (whenM)
import qualified Data.ByteString                    as BS
import qualified Data.ByteString.Lazy               as BL
import           Data.Conduit                       (Sink, Source, ($$), (=$=), yield, await)
import           Data.Conduit.Binary                (sinkLbs, sourceLbs)
import           Data.Conduit.Network               (sinkSocket, sourceSocket)
import           Data.Conduit.TMChan                (sinkTBMChan, sourceTBMChan)
import           Data.Default                       (Default (..))
import           Data.HashMap.Strict                (HashMap)
import qualified Data.IORef                         as IR
import           Data.List                          (intersperse)
import           Data.Streaming.Network             (acceptSafe, bindPortTCP,
                                                     getSocketFamilyTCP, safeRecv)
import           Data.Text                          (Text)
import qualified Data.Text                          as T
import           Data.Text.Buildable                (Buildable (build), build)
import           Data.Text.Encoding                 (decodeUtf8)
import           Data.Typeable                      (Typeable)
import           Formatting                         (bprint, builder, int, sformat, shown,
                                                     stext, string, (%), float)
import qualified Network.Socket                     as NS
import           Serokell.Util.Base                 (inCurrentContext)
import           Serokell.Util.Concurrent           (modifyTVarS)
import           System.Wlog                        (CanLog, HasLoggerName, LoggerNameBox,
                                                     Severity (..), WithLogger, logDebug,
                                                     logInfo, logMessage, logWarning,
                                                     logError)

import           Control.TimeWarp.Manager           (InterruptType (..), JobCurator (..),
                                                     addManagerAsJob, addSafeThreadJob,
                                                     addThreadJob, addThreadJobLabeled,
                                                     interruptAllJobs,
                                                     isInterrupted, jcIsClosed,
                                                     mkJobCurator, stopAllJobs,
                                                     unlessInterrupted)
import           Control.TimeWarp.Rpc.MonadTransfer (Binding (..), MonadTransfer (..),
                                                     NetworkAddress, Port,
                                                     ResponseContext (..), ResponseT,
                                                     commLog, runResponseT, runResponseT,
                                                     sendRaw)
import           Control.TimeWarp.Timed             (Microsecond, MonadTimed, ThreadId,
                                                     TimedIO, for, fork, fork_, interval,
                                                     forkLabeled, forkLabeled_,
                                                     killThread, sec, wait)
import           GHC.Stats                          (getGCStats, GCStats(..))
import           System.Mem                         (performGC)
import           Debug.Trace                        (traceEventIO)
import           System.IO.Unsafe                   (unsafePerformIO)

-- * Util

-- | Like sinkTBMChan from the library but this one will log when the channel
--   is full.
sinkTBMChan'
  :: forall m msg .
     ( Show msg, CanLog m, HasLoggerName m, MonadIO m )
  => msg
  -> TBM.TBMChan BS.ByteString
  -> Bool
  -> Sink BS.ByteString m ()
sinkTBMChan' msg chan shouldClose = loop >> closer
  where
  loop = do
    maybeNext <- await
    case maybeNext of
      Nothing -> pure ()
      Just x -> do
        wasWritten <- liftIO . atomically $ TBM.tryWriteTBMChan chan x
        case wasWritten of
          Nothing -> pure ()
          Just False -> do
            -- Log and then do a potentially blocking write
            let bsSize = BS.length x
            lift . commLog . logInfo $
                sformat ("sinkTBMChan' blocking on full channel with pending data of size " % int % ". " % shown) bsSize msg
            liftIO . atomically $ TBM.writeTBMChan chan x
            loop
          Just True -> loop
  closer = when shouldClose (liftIO . atomically $ TBM.closeTBMChan chan)


logSeverityUnlessClosed :: (WithLogger m, MonadIO m)
                        => Severity -> JobCurator -> Text -> m ()
logSeverityUnlessClosed severityIfNotClosed jm msg = do
    closed <- isInterrupted jm
    let severity = if closed then severityIfNotClosed else Debug
    logMessage severity msg


-- * Related datatypes

-- ** Exceptions

-- | Error thrown if attempt to listen at already being listened connection is performed.
data TransferException = AlreadyListeningOutbound Text
    deriving (Show, Typeable)

instance Exception TransferException

instance Buildable TransferException where
    build (AlreadyListeningOutbound addr) =
        bprint ("Already listening at outbound connection to "%stext) addr

-- | Error thrown if peer was detected to close connection.
data PeerClosedConnection = PeerClosedConnection
    deriving (Show, Typeable)

instance Exception PeerClosedConnection

instance Buildable PeerClosedConnection where
    build _ = "Peer closed connection"

-- ** Connections

-- | Textual representation of peer node. For debugging purposes only.
type PeerAddr = Text

data OutputConnection = OutputConnection
    { outConnSend       :: forall m . (MonadIO m, MonadMask m, WithLogger m)
                        => Source m BS.ByteString -> m ()
      -- ^ Function to send all data produced by source
    , outConnRec        :: forall m . (MonadIO m, MonadMask m, MonadTimed m,
                                       MonadBaseControl IO m, WithLogger m)
                        => Sink BS.ByteString (ResponseT m) () -> m ()
      -- ^ Function to stark sink-listener, returns synchronous closer
    , outConnJobCurator :: JobCurator
      -- ^ Job manager for this connection
    , outConnAddr       :: PeerAddr
      -- ^ Address of socket on other side of net
    }


-- ** Settings

-- | Number of consequent fails while trying to establish connection.
type FailsInRow = Int

data Settings = Settings
    { queueSize       :: Int
    , reconnectPolicy :: forall m . (HasLoggerName m, MonadIO m)
                      => FailsInRow -> m (Maybe Microsecond)
    }

-- | Default settings, you can use it like @transferSettings { queueSize = 1 }@
instance Default Settings where
    def = Settings
        { queueSize = 100
        , reconnectPolicy =
            \failsInRow -> return $ guard (failsInRow < 3) >> Just (interval 3 sec)
        }


-- ** ConnectionPool

newtype ConnectionPool = ConnectionPool
    { _outputConn :: HashMap NetworkAddress OutputConnection
    }

makeLenses ''ConnectionPool

initConnectionPool :: ConnectionPool
initConnectionPool =
    ConnectionPool
    { _outputConn = mempty
    }

-- ** SocketFrame

-- | Keeps data required to implement so-called /lively socket/.
data SocketFrame = SocketFrame
    { sfPeerAddr   :: PeerAddr
    -- ^ Peer address, for debuging purposes only
    , sfInBusy     :: TV.TVar Bool
    -- ^ Whether someone already listens on this socket
    , sfInChan     :: TBM.TBMChan BS.ByteString
    -- ^ For incoming packs of bytes
    , sfOutChan    :: TBM.TBMChan (BL.ByteString, IO ())
    -- ^ For (packs of bytes to send, notification when bytes passed to socket)
    , sfJobCurator :: JobCurator
    -- ^ Job manager, tracks whether lively-socket wasn't closed.
    }

mkSocketFrame :: MonadIO m => Settings -> PeerAddr -> m SocketFrame
mkSocketFrame settings sfPeerAddr = liftIO $ do
    sfInBusy     <- TV.newTVarIO False
    sfInChan     <- TBM.newTBMChanIO (queueSize settings)
    sfOutChan    <- TBM.newTBMChanIO (queueSize settings)
    sfJobCurator <- mkJobCurator
    return SocketFrame{..}

-- | Makes sender function in terms of @MonadTransfer@ for given `SocketFrame`.
-- This first extracts ready `Lazy.ByteString` from given source, and then passes it to
-- sending queue.
sfSend :: (MonadIO m, WithLogger m)
       => SocketFrame -> Source m BS.ByteString -> m ()
sfSend SocketFrame{..} src = do
    let jm = getJobCurator sfJobCurator
    alreadyClosed <- liftIO . atomically $ view jcIsClosed <$> TV.readTVar jm
    if alreadyClosed
    then commLog . logWarning $ sformat ("sfSend : SocketFrame for " % shown % " is already closed") sfPeerAddr
    else do
        lbs <- src $$ sinkLbs
        logQueueState
        (notifier, awaiter) <- mkMonitor
        liftIO . atomically . TBM.writeTBMChan sfOutChan $ (lbs, atomically notifier)

        -- wait till data get consumed by socket, but immediatelly quit on socket
        -- get closed.
        --
        -- This will block until somebody hits 'notifier', or the job curator is
        -- closed, so we must take care to ensure that, eventually, one of these
        -- happens.
        --
        -- A note on the BlockedIndefinitelyOnSTM. This should come up in case
        -- all other references to TVar in mkMonitor are lost. There's one other
        -- reference, namely the one in the TBMChan that we wrote just above. So
        -- if it's cleared from the queue and the notifier is not run, we should
        -- get that exception. However, if it's not cleared from the queue, and
        -- the SocketFrame is retained in some other thread, we'll just wait here.
        -- 'checkClosed' will retry if the job curator is not closed.
        -- 'awaiter', as defined in 'mkMonitor', follows the same motif.
        -- We combine them with 'orElse' to get an STM which will retry until
        -- either the job curator is closed or the notifier is called.
        let checkClosed = check =<< (view jcIsClosed <$> TV.readTVar jm)
        let waitNotifyOrClose = const () <$> (checkClosed `orElse` awaiter)
        liftIO . atomically $ waitNotifyOrClose
  where
    -- creates pair (@notifier@, @awaiter@), where @awaiter@ blocks thread
    -- until @notifier@ is called.
    mkMonitor = do
        t <- liftIO $ TV.newTVarIO False
        return ( TV.writeTVar t True
               , check =<< TV.readTVar t
               )

    logQueueState = do
        whenM (liftIO . atomically $ TBM.isFullTBMChan sfOutChan) $
            commLog . logWarning $
                sformat ("Send channel for "%shown%" is full") sfPeerAddr
        whenM (liftIO . atomically $ TBM.isClosedTBMChan sfOutChan) $
            commLog . logWarning $
                sformat ("Send channel for "%shown%" is closed, message wouldn't be sent")
                    sfPeerAddr

-- | Constructs function which allows to infinitelly listen on given `SocketFrame`
-- in terms of `MonadTransfer`.
-- Attempt to use this function twice will end with `AlreadyListeningOutbound` error.
sfReceive :: (MonadIO m, MonadMask m, MonadTimed m, WithLogger m,
              MonadBaseControl IO m)
          => SocketFrame -> Sink BS.ByteString (ResponseT m) () -> m ()
sfReceive sf@SocketFrame{..} sink = do
    -- This is dubious. What if we give a different Sink from the last
    -- use of sfReceive?
    busy <- liftIO . atomically $ TV.swapTVar sfInBusy True
    when busy $ throwM $ AlreadyListeningOutbound sfPeerAddr

    liManager <- mkJobCurator
    onTimeout <- inCurrentContext logOnInterruptTimeout
    let interruptType = WithTimeout (interval 3 sec) onTimeout
    addManagerAsJob sfJobCurator interruptType liManager
    -- FIXME
    --
    -- it is observed that this thread repeatedly dies because it's blocked
    -- indefinitely on an STM transaction. Surely that's to say, the
    -- sfInChan is not reachable from any other thread. How could this
    -- come about? The SocketFrame must be removed from the pool, but that
    -- happens only if the connection times out, no?
    --
    -- We also observe that sinkTBMChan' regularly blocks on a full channel.
    -- Right here, in 'sfReceive', seems to be the only place where we
    -- clear that channel. But if the other thread is blocked on that chan,
    -- then this thread wouldn't die due to blocked indefinitely; rather, it
    -- would take from the chan and dump to the socket.
    --
    -- It could be that the sink throws that exception. When applied to the
    -- pos prototype, the sink is running nontrivial listeners, and it's
    -- plausible that one of these could get blocked on STM.
    --
    -- Anyway, if we get an exception here, can we simply interruptAllJobs?
    -- Will that cause the in and out channels to be collected?
    addThreadJobLabeled "sourceTBMChan" liManager $ logOnErr $ threadJob
    pure ()
  where
    threadJob = do
        (sourceTBMChan sfInChan $$ sink) `runResponseT` sfMkResponseCtx sf
        logListeningHappilyStopped

    logOnErr = handleAll $ \e -> do
        commLog . logWarning $ sformat ("Server error: " % shown) e
        unlessInterrupted sfJobCurator $ interruptAllJobs sfJobCurator Plain

    logOnInterruptTimeout = commLog . logInfo $
        sformat ("While closing socket to "%stext%" listener "%
                 "worked for too long, closing with no regard to it") sfPeerAddr

    logListeningHappilyStopped =
        commLog . logInfo $
            sformat ("Listening on socket to "%stext%" happily stopped") sfPeerAddr

sfClose :: SocketFrame -> IO ()
sfClose SocketFrame{..} = do
    interruptAllJobs sfJobCurator Plain
    atomically $ do
        TBM.closeTBMChan sfInChan
        TBM.closeTBMChan sfOutChan
        clearInChan
  where
    clearInChan = TBM.tryReadTBMChan sfInChan >>= maybe (return ()) (const clearInChan)

sfMkOutputConn :: SocketFrame -> OutputConnection
sfMkOutputConn sf =
    OutputConnection
    { outConnSend       = sfSend sf
    , outConnRec        = sfReceive sf
    , outConnJobCurator = sfJobCurator sf
    , outConnAddr       = sfPeerAddr sf
    }

sfMkResponseCtx :: SocketFrame -> ResponseContext
sfMkResponseCtx sf =
    ResponseContext
    { respSend     = sfSend sf
    , respClose    = sfClose sf
    , respPeerAddr = sfPeerAddr sf
    }

-- | Starts workers, which connect channels in `SocketFrame` with real `NS.Socket`.
-- If error in any worker occurs, it's propagated.
sfProcessSocket
  :: forall m . (MonadIO m, MonadMask m, MonadTimed m, WithLogger m)
  => SocketFrame -> NS.Socket -> m ()
sfProcessSocket SocketFrame{..} sock = do
    -- Time out and raise an exception after 3 seconds.
    -- This is important. If we make a connection but the peer just leaves it
    -- open and never tries to receive any data, our queues will back up and
    -- our heap will balloon.
    _ <- liftIO $ NS.setSocketOption sock NS.UserTimeout 3000
    -- TODO: rewrite to async when MonadTimed supports it
    -- create channel to notify about error
    eventChan  <- liftIO TC.newTChanIO
    -- create worker threads
    stid <- forkLabeled "sfProcessSocket send" $ reportErrors eventChan foreverSend $
        sformat ("foreverSend on "%stext) sfPeerAddr
    rtid <- forkLabeled "sfProcessSocket receive" $ reportErrors eventChan foreverRec $
        sformat ("foreverRec on "%stext) sfPeerAddr
    commLog . logInfo $ sformat ("Start processing of socket to "%stext) sfPeerAddr
    -- check whether @isClosed@ keeps @True@
    ctid <- fork $ do
        let jm = getJobCurator sfJobCurator
        liftIO . atomically $ check . view jcIsClosed =<< TV.readTVar jm
        liftIO . atomically $ TC.writeTChan eventChan $ Right ()
    let exceptionHandler = \e -> do
            logError $ sformat ("sfProcessSocket : error while waiting for signal " % shown) e
            pure (Left e)
    event <- (liftIO . atomically $ TC.readTChan eventChan) `catchAll` exceptionHandler
    -- NB the thread which didn't finish will get a ThreadKilled
    -- exception and it'll crop up in the chan via reportErrors.
    mapM_ killThread [stid, rtid, ctid]
    commLog . logInfo $ sformat ("Stop processing socket to "%stext) sfPeerAddr
    -- Left - worker error, Right - get closed
    either throwM return event
    -- at this point workers are stopped
  where
    foreverSend = do
        datm <- liftIO . atomically $ TBM.readTBMChan sfOutChan
        forM_ datm $
            \dat@(bs, notif) -> do
                -- Potential issue here.
                -- Here's what we previously had:
                --
                --let pushback = liftIO . atomically $ TBM.unGetTBMChan sfOutChan dat
                --unmask (sourceLbs bs $$ sinkSocket sock) `onException` pushback
                --
                -- Here we drop the data which failed to send. Perhaps we
                -- could still hold onto it (TBM.unGetTBMChan) so long as
                -- we expect that, after some reasonable amount of time,
                -- the data will either go down the wire, or we'll give up
                -- and let it be collected.
                let logException = commLog . logInfo $
                      sformat ("foreverSend got exception, dropping data of size " % int) (BL.length bs)
                (sourceLbs bs $$ sinkSocket sock) `onException` logException
                -- TODO: if get async exception here   ^, will send msg twice
                --
                -- FIXME ?
                -- If an exception is raised, we don't call notif.
                -- Does this mean that an sfSend on this frame (which sinks
                -- to sfOutChan) will block indefinitely? The notifier is
                -- still alive (in the TBMChan, we push it back on), so GHC
                -- wouldn't say blocked indefinitely on an STM unless the
                -- SocketFrame itself goes away.
                liftIO notif
                foreverSend

    foreverRec :: m ()
    foreverRec = do
        -- FIXME
        -- it is observed that sinkTBMChan' often blocks on a full channel.
        -- This is bad. It means we'll happily pull data in from the socket even
        -- though we have nowhere to put it, and our heap will grow as big as an
        -- attacker would like for it to grow.
        --
        -- Who is clearing 'sfInChan'? 'sfReceive' is, and it's using a sink
        -- provided by the user. We observe that the conduit in 'sfReceive'
        -- blocks indefinitely on an STM. Perhaps it's not the channel which
        -- causes the exception.
        sourceSocket sock $$ sinkTBMChan' sfPeerAddr sfInChan False
        unlessInterrupted sfJobCurator $
            throwM PeerClosedConnection

    reportErrors eventChan action desc =
        action `catchAll` \e -> do
            commLog . logInfo $ sformat ("Caught error on "%stext%": " % shown) desc e
            liftIO . atomically . TC.writeTChan eventChan . Left $ e


-- * Transfer

newtype Transfer a = Transfer
    { getTransfer :: ReaderT Settings
                        (ReaderT (TV.TVar ConnectionPool)
                            (LoggerNameBox
                                TimedIO
                            )
                        ) a
    } deriving (Functor, Applicative, Monad, MonadIO, MonadBase IO,
                MonadThrow, MonadCatch, MonadMask, MonadTimed, CanLog, HasLoggerName)

type instance ThreadId Transfer = C.ThreadId

-- | Run with specified settings.
runTransferS :: Settings -> Transfer a -> LoggerNameBox TimedIO a
runTransferS s t = do m <- liftIO (TV.newTVarIO initConnectionPool)
                      flip runReaderT m $ flip runReaderT s $ getTransfer t

runTransfer :: Transfer a -> LoggerNameBox TimedIO a
runTransfer = runTransferS def

modifyManager :: StateT ConnectionPool STM a -> Transfer a
modifyManager how = Transfer . lift $
    ask >>= liftIO . atomically . flip modifyTVarS how


-- * Logic

buildSockAddr :: NS.SockAddr -> PeerAddr
buildSockAddr (NS.SockAddrInet port host) =
    let buildHost = mconcat . intersperse "."
                  . map build . (^.. each) . NS.hostAddressToTuple
    in  sformat (builder%":"%int) (buildHost host) port

buildSockAddr (NS.SockAddrInet6 port _ host _) =
    let buildHost6 = mconcat . intersperse "."
                   . map build . (^.. each) . NS.hostAddress6ToTuple
    in  sformat (builder%":"%int) (buildHost6 host) port

buildSockAddr (NS.SockAddrUnix addr) = sformat string addr

buildSockAddr (NS.SockAddrCan addr) = sformat ("can:"%int) addr

buildNetworkAddress :: NetworkAddress -> PeerAddr
buildNetworkAddress (host, port) = sformat (stext%":"%int) (decodeUtf8 host) port

-- | Binds TCP socket on a given port (any host).
--   A thread job is added to a JobCurator. This thread uses `acceptSafe` to
--   accept a connection on the socket and once it does, it spawns a thread
--   of its own which dumps the socket data to the sink.
listenInbound :: Port
              -> Sink BS.ByteString (ResponseT Transfer) ()
              -> Transfer (Transfer ())
listenInbound (fromIntegral -> port) sink = do
    commLog . logInfo $ sformat ("Starting server at "%int) port
    serverJobCurator <- mkJobCurator
    -- launch server
    bracketOnError (liftIO $ bindPortTCP port "*") (liftIO . NS.close) $
        \lsocket -> addThreadJobLabeled "listenInbound" serverJobCurator $
            flip finally (liftIO $ NS.close lsocket) $
                handleAll (logOnServerError serverJobCurator) $
                   serve lsocket serverJobCurator
    -- return closer
    inCurrentContext $ do
        commLog . logInfo $ sformat ("Stopping server at "%int) port
        stopAllJobs serverJobCurator
        commLog . logInfo $ sformat ("Server at "%int%" fully stopped") port
  where
    serve lsocket serverJobCurator = forever $
        bracketOnError (acceptAndTrace lsocket) closeAndTrace $
            \(sock, addr) -> forkLabeled_ "listenInbound accepted connection" $ do
                settings <- Transfer ask
                sf@SocketFrame{..} <- mkSocketFrame settings $ buildSockAddr addr
                addManagerAsJob serverJobCurator Plain sfJobCurator

                logNewInputConnection sfPeerAddr
                (processSocket sock sf serverJobCurator)
                    `finally` (closeAndTrace (sock, addr))

    acceptAndTrace lsocket = do
      (sock, addr) <- liftIO $ NS.accept lsocket
      () <- liftIO performGC
      stats <- liftIO getGCStats
      let cpuTime = cpuSeconds stats
      let bytes = fromIntegral (currentBytesUsed stats) :: Int
      let readableAddr = buildSockAddr addr
      commLog . logInfo $ sformat ("trace cpuTime: " % float % ", bytes in heap: " % int % " : accepted connection to " % stext) cpuTime bytes readableAddr
      liftIO . traceEventIO . T.unpack $ sformat ("START connection " % stext) readableAddr
      pure (sock, addr)

    closeAndTrace (sock, addr) = do
      liftIO . NS.close $ sock
      () <- liftIO performGC
      stats <- liftIO getGCStats
      let cpuTime = cpuSeconds stats
      let bytes = fromIntegral (currentBytesUsed stats) :: Int
      let readableAddr = buildSockAddr addr
      commLog . logInfo $ sformat ("trace cpuTime: " % float % " bytes in heap: " % int % " : closed connection to " % stext) cpuTime bytes readableAddr
      liftIO . traceEventIO . T.unpack $ sformat ("STOP connection " %stext) readableAddr
      pure ()

    -- makes socket work, finishes once it's fully shutdown
    processSocket sock sf@SocketFrame{..} jc = do
        liftIO $ NS.setSocketOption sock NS.ReuseAddr 1
        liftIO $ NS.setSocketOption sock NS.ReusePort 1
        -- sfReceive forks, does not block.
        -- It pulls data from the socket frame's in channel, which is fed by
        -- the socket (see sfProcessSocket).
        unlessInterrupted jc $ do
            sfReceive sf sink
            handleAll (logErrorOnServerSocketProcessing jc sfPeerAddr) $ do
                -- sfProcessSocket blocks until it gets an event.
                --
                -- NB 'sfProcessSocket' will pull from the socket into the
                -- 'sfInChan' and also pull from 'sfOutChan' into the socket.
                -- But 'sfOutChan' in practice will never be fed any data, for
                -- we're merely *listening* on this 'SocketFrame'.
                -- This shouldn't leak memory, though.
                sfProcessSocket sf sock
                logInputConnHappilyClosed sfPeerAddr

    -- * Logs

    logNewInputConnection addr =
        commLog . logInfo $
            sformat ("New input connection: "%int%" <- "%stext)
            port addr

    logErrorOnServerSocketProcessing jm addr e =
        logSeverityUnlessClosed Warning jm $
            sformat ("Error in server socket "%int%" connected with "%stext%": "%shown)
            port addr e

    logOnServerError jm e =
        logSeverityUnlessClosed Error jm $
            sformat ("Server at port "%int%" stopped with error "%shown) port e

    logInputConnHappilyClosed addr =
        commLog . logInfo $
            sformat ("Happily closing input connection "%int%" <- "%stext)
            port addr


-- | Listens for incoming bytes on outbound connection.
-- This thread doesn't block current thread. Use returned function to close relevant
-- connection.
listenOutbound :: NetworkAddress
               -> Sink BS.ByteString (ResponseT Transfer) ()
               -> Transfer (Transfer ())
listenOutbound addr sink = do
    conn <- getOutConnOrOpen addr
    outConnRec conn sink
    return $ stopAllJobs $ outConnJobCurator conn

-- | Grabs an existing output connection or creates a new one. If a new one
--   is created, a thread is spawned which will attempt to get a TCP connection
--   to the given address by way of 'getSocketFamilyTCP'. When a connection is
--   established, 'sfProcessSocket' goes to work. If it throws *any* exception,
--   bracket will close the socket, and then attempt(s) are made to reconnect
--   subject to a limit defined in the 'Settings' of the 'Transfer' monad.
getOutConnOrOpen :: NetworkAddress -> Transfer OutputConnection
getOutConnOrOpen addr@(host, fromIntegral -> port) =
    -- Obtain an 'OutputConnection', possibly an existing one for this address.
    -- If there's no existing connection, make a new 'SocketFrame' and
    -- corresponding 'OutputConnection' and spawn a thread to make it "lively".
    -- When that thread dies, release the 'SocketFrame' from the pool.
    --
    -- Suppose the 'SocketFrame' is released (the worker thread finishes,
    -- whether normally or exceptionally). The entry for this address in the
    -- pool will be cleared, but an 'OutputConnection' derived from that
    -- 'SocketFrame' may still be around! The caller of this function gets a
    -- hold of it. It retains the input and output 'TBMChan's for the dead
    -- 'SocketFrame' (nobody is clearing these).
    -- The 'OutputConnection' also retains the job curator of the 'SocketFrame',
    -- and when the latter is released, 'interruptAllJobs' is called on that
    -- curator.
    --
    do (conn, sfm) <- ensureConnExist
       -- If it's a freshly minted 'SocketFrame' we'll spawn a thread to
       -- hook its queues up to a TCP connection.
       -- We release the 'SocketFrame' and its queues only if the worker
       -- ends (normally or exceptionally).
       --
       -- FIXME 'addSafeThreadJob' is perhaps not appropriate.
       -- This variant uses an "interrupter" which does nothing, so
       -- interrupting all jobs will not kill the thread and release the
       -- connection as it would if we chose 'addThreadJob'
       forM_ (sfm :: Maybe SocketFrame) $
           \sf -> addThreadJob (sfJobCurator sf) $
               startWorker sf `finally` releaseConn sf
       return conn
  where
    addrName = buildNetworkAddress addr

    -- Checks a pool for an existing connection to the given address.
    -- It gives an 'OutputConnection' always, and if it's new (not for an
    -- existing connection) then its underlying 'SocketFrame' is also given.
    ensureConnExist = do
        settings <- Transfer ask
        let getOr m act = maybe act (return . (, Nothing)) m

        -- two-phase connection creation
        -- 1. check whether connection already exists: if doesn't, make `SocketFrame`.
        -- 2. check again, if still absent push connection to pool
        mconn <- modifyManager $ use $ outputConn . at addr
        getOr mconn $ do
            sf <- mkSocketFrame settings addrName
            let conn = sfMkOutputConn sf
            modifyManager $ do
                mres <- use $ outputConn . at addr
                getOr mres $ do
                    outputConn . at addr ?= conn
                    return (conn, Just sf)

    startWorker sf = do
        failsInRow <- liftIO $ IR.newIORef 0
        commLog . logInfo $ sformat ("Lively socket to "%stext%" created, processing")
            (sfPeerAddr sf)
        withRecovery sf failsInRow

    establishConn sf failsInRow =
        bracket (liftIO $ fst <$> getSocketFamilyTCP host port NS.AF_UNSPEC)
                (liftIO . NS.close) $
                \sock -> do
                    commLog . logInfo $
                        sformat ("established connection to " % stext) (sfPeerAddr sf)
                    -- NB we do *not* reset the failsInRow counter just because
                    -- we have connected. It could be that, for instance, the
                    -- peer accepts our connection but never tries to receive
                    -- any data, in which case (due to UserTimeout) we would
                    -- close the socket and try again forever, resetting the
                    -- failure counter each time.
                    -- _ <- liftIO $ IR.atomicWriteIORef failsInRow 0
                    sfProcessSocket sf sock

    -- Repeatedly try to establish a TCP connection, giving up after a set
    -- number of exceptions as determined by the 'Settings' of this 'Transfer'
    withRecovery sf failsInRow = catchAll (establishConn sf failsInRow) $ \e -> do
        closed <- isInterrupted (sfJobCurator sf)
        unless closed $ do
            commLog . logWarning $
                sformat ("Error while working with socket to "%stext%": "%shown)
                    addrName e
            reconnect <- reconnectPolicy <$> Transfer ask
            fails <- liftIO $ succ <$> IR.readIORef failsInRow
            liftIO $ IR.atomicWriteIORef failsInRow fails
            maybeReconnect <- reconnect fails
            case maybeReconnect of
                Nothing ->
                    commLog . logWarning $
                        sformat ("Can't connect to "%shown%", aborting connection") addr
                Just delay -> do
                    commLog . logWarning $
                        sformat ("Reconnect to "%shown%" in "%shown) addr delay
                    wait (for delay)
                    withRecovery sf failsInRow

    releaseConn sf = do
        commLog . logInfo $
            sformat ("releasing connection to " % stext) addrName
        interruptAllJobs (sfJobCurator sf) Plain
        commLog . logInfo $
            sformat ("almost released connection to " % stext) addrName
        modifyManager $ outputConn . at addr .= Nothing
        commLog . logInfo $
            sformat ("successfully released connection to " % stext) addrName

instance MonadTransfer Transfer where
    -- An idea for a simplification.
    -- Why not just try to pull the SocketFrame itself from a pool (or create
    -- anew if needed) and then send directly through it, blocking on the
    -- send? It's no worse than what we have now, just simpler; currently
    -- we still wait on send (via socketSink) but we do so indirectly, via
    -- a notifier TVar which the sfProcessSink sending thread must call.
    -- This alternative is safer. If the socket blows up, you'll hear about
    -- it, because you're blocked on send.
    sendRaw addr src = do
        conn <- getOutConnOrOpen addr
        outConnSend conn src

    listenRaw (AtPort   port) = listenInbound port
    listenRaw (AtConnTo addr) = listenOutbound addr

    -- closes asynchronuosly
    close addr = do
        maybeConn <- modifyManager . use $ outputConn . at addr
        forM_ maybeConn $
            \conn -> interruptAllJobs (outConnJobCurator conn) Plain


-- * Instances

instance MonadBaseControl IO Transfer where
    type StM Transfer a = StM (ReaderT (TV.TVar ConnectionPool) TimedIO) a
    liftBaseWith io =
        Transfer $ liftBaseWith $ \runInBase -> io $ runInBase . getTransfer
    restoreM = Transfer . restoreM
