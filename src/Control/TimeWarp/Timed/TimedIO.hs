{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE Rank2Types            #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE ViewPatterns          #-}

-- |
-- Module      : Control.TimeWarp.Timed.TimedIO
-- Copyright   : (c) Serokell, 2016
-- License     : GPL-3 (see the file LICENSE)
-- Maintainer  : Serokell <hi@serokell.io>
-- Stability   : experimental
-- Portability : POSIX, GHC
--
-- Real-mode implementation of `MonadTimed`.
-- Each function in inplementation refers to plain `IO`.

module Control.TimeWarp.Timed.TimedIO
       ( TimedIO
       , runTimedIO
       ) where

import qualified Control.Concurrent                as C
import           Control.Monad.Base                (MonadBase)
import           Control.Monad.Catch               (MonadCatch, MonadMask, MonadThrow,
                                                    throwM)
import           Control.Monad.Reader              (ReaderT (..), ask, runReaderT)
import           Control.Monad.Trans               (MonadIO, lift, liftIO)
import           Control.Monad.Trans.Control       (MonadBaseControl, StM, liftBaseWith,
                                                    restoreM)
import           Data.Time.Clock.POSIX             (getPOSIXTime)
import           Data.Time.Units                   (toMicroseconds)
import           SlaveThread                       as ST
import qualified System.Timeout                    as T
import           System.Wlog                       (CanLog)
import qualified GHC.Conc                          (labelThread)

import           Control.TimeWarp.Timed.MonadTimed (Microsecond, MonadTimed (..),
                                                    MonadTimedError (MTTimeoutError),
                                                    ThreadId)

-- | Default implementation for `IO`, i.e. real mode.
-- `wait` refers to `Control.Concurrent.threadDelay`,
-- `fork` refers to `Control.Concurrent.forkIO`, and so on.
newtype TimedIO a = TimedIO
    { getTimedIO :: ReaderT Microsecond IO a
    } deriving (Functor, Applicative, Monad, MonadIO, MonadThrow, MonadCatch,
               MonadBase IO, MonadMask, CanLog)

instance MonadBaseControl IO TimedIO where
    type StM TimedIO a = a

    liftBaseWith f = TimedIO $ liftBaseWith $ \g -> f $ g . getTimedIO

    restoreM = TimedIO . restoreM

type instance ThreadId TimedIO = C.ThreadId

instance MonadTimed TimedIO where
    virtualTime = TimedIO $ (-) <$> lift curTime <*> ask

    currentTime = TimedIO $ lift curTime

    wait relativeToNow = do
        cur <- virtualTime
        liftIO $ C.threadDelay $ fromIntegral $ relativeToNow cur - cur

    fork (TimedIO a) = TimedIO $ lift . C.forkIO . runReaderT a =<< ask

    labelThread tid name = liftIO $ GHC.Conc.labelThread tid name

    myThreadId = TimedIO $ lift $ C.myThreadId

    throwTo tid e = TimedIO $ lift $ C.throwTo tid e

    timeout (toMicroseconds -> t) (TimedIO action) = TimedIO $ do
        res <- liftIO . T.timeout (fromIntegral t) . runReaderT action =<< ask
        maybe (throwM $ MTTimeoutError "Timeout has exceeded") return res

    forkSlave (TimedIO a) = TimedIO $ lift . ST.fork . runReaderT a =<< ask

    mkWeakThreadId = TimedIO . lift . C.mkWeakThreadId

-- | Launches scenario using real time and threads.
runTimedIO :: TimedIO a -> IO a
runTimedIO = (curTime >>= ) . runReaderT . getTimedIO

curTime :: IO Microsecond
curTime = round . ( * 1000000) <$> getPOSIXTime
