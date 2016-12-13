{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE ViewPatterns          #-}
{-# LANGUAGE ScopedTypeVariables   #-}

-- | This module provides abstractions for parallel job processing.

module Control.TimeWarp.Manager.Job
       ( -- * Job data types
         InterruptType (..)
       , JobCurator    (..)

         -- * 'JobsState' lenses
       , jcCounter
       , jcIsClosed
       , jcJobs

         -- * Manager utilities
       , addManagerAsJob
       , addSafeThreadJob
       , addThreadJobGeneral
       , addThreadJob
       , addThreadJobLabeled
       , interruptAllJobs
       , isInterrupted
       , mkJobCurator
       , stopAllJobs
       , unlessInterrupted
       ) where

import           Control.Concurrent          (forkIO)
import           Control.Concurrent.STM      (atomically, check)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVar, readTVarIO,
                                              writeTVar)
import           Control.Lens                (at, makeLenses, use, view, (&), (.=), (.~),
                                              (<<+=), (<<.=), (?=), (^.))
import           Control.Monad               (mapM_, unless, void)
import           Control.Monad.Catch         (MonadMask (mask), finally)
import           Control.Monad.Extra         ((&&^))
import           Control.Monad.Trans         (MonadIO (liftIO))
import           Control.Monad.Trans.Control (MonadBaseControl (..))

import           Data.Hashable               (Hashable)
import           Data.HashMap.Strict         (HashMap)
import qualified Data.HashMap.Strict         as HM hiding (HashMap)
import           Serokell.Util.Base          (inCurrentContext)
import           Serokell.Util.Concurrent    (modifyTVarS, threadDelay)
import           System.Wlog                 (CanLog, HasLoggerName, logInfo)
import           Formatting                  (sformat, (%), string)

import           Control.TimeWarp.Timed      (Microsecond, MonadTimed, fork_,
                                              forkLabeled_, killThread,
                                              myThreadId, mkWeakThreadId,
                                              showThreadId)
import qualified System.Mem.Weak             as Weak
import           Data.Proxy                  (Proxy(Proxy))

-- | Unique identifier of job.
newtype JobId = JobId Word
    deriving (Show, Eq, Num, Hashable)

-- | Job killer.
newtype JobInterrupter = JobInterrupter
    { runJobInterrupter :: IO ()
    }

-- | Action to mark job as finished
newtype MarkJobFinished = MarkJobFinished
    { runMarker :: IO ()
    }

data JobCuratorState = JobCuratorState
    { -- | @True@ if interrupt had been invoked. Also, when @True@, no job could be added
      _jcIsClosed :: !Bool

      -- | 'Map' with currently active jobs
    , _jcJobs     :: !(HashMap JobId JobInterrupter)

      -- | Total number of allocated jobs ever
    , _jcCounter  :: !JobId
    }

makeLenses ''JobCuratorState

-- | Keeps set of jobs. Allows to stop jobs and wait till all of them finish.
newtype JobCurator = JobCurator
    { getJobCurator :: TVar JobCuratorState
    }

-- | Defines way to interrupt all jobs in curator.
data InterruptType
    = Plain
    -- ^ Just interrupt all jobs
    | Force
    -- ^ Interrupt all jobs, and treat them all as completed
    | WithTimeout !Microsecond !(IO ())
    -- ^ Interrupt all jobs in `Plain` was, but if some jobs fail to complete in time,
    -- interrupt `Force`ly and execute given action.

mkJobCurator :: MonadIO m => m JobCurator
mkJobCurator = JobCurator <$> (liftIO $ newTVarIO
    JobCuratorState
    { _jcIsClosed = False
    , _jcJobs     = mempty
    , _jcCounter  = 0
    })


-- | Remembers and starts given action.
-- Once `interruptAllJobs` called on this manager, if job is not completed yet,
-- `JobInterrupter` is invoked.
--
-- Given job *must* invoke given `MarkJobFinished` upon finishing, even if it was
-- interrupted.
--
-- If curator is already interrupted, action would not start, and `JobInterrupter`
-- would be invoked.
addJob :: MonadIO m
       => JobCurator
       -> JobInterrupter
       -> (MarkJobFinished -> m ())
       -> m ()
addJob
       (getJobCurator     -> curator)
    ji@(runJobInterrupter -> interrupter)
    action
  = do
    jidm <- liftIO . atomically $ do
        st <- readTVar curator
        let closed = st ^. jcIsClosed
        if closed
            then return Nothing
            else modifyTVarS curator $ do
                    no <- jcCounter <<+= 1
                    jcJobs . at no ?= ji
                    return $ Just no
    maybe (liftIO interrupter) (action . markReady) jidm
  where
    markReady jid = MarkJobFinished $ atomically $ do
        st <- readTVar curator
        writeTVar curator $ st & jcJobs . at jid .~ Nothing

-- | Invokes `JobInterrupter`s for all incompleted jobs.
-- Has no effect on second call.
interruptAllJobs :: MonadIO m => JobCurator -> InterruptType -> m ()
interruptAllJobs (getJobCurator -> curator) Plain = do
    jobs <- liftIO . atomically $ modifyTVarS curator $ do
        wasClosed <- jcIsClosed <<.= True
        if wasClosed
            then return mempty
            else use jcJobs
    liftIO $ mapM_ runJobInterrupter jobs
interruptAllJobs c@(getJobCurator -> curator) Force = do
    interruptAllJobs c Plain
    liftIO . atomically $ modifyTVarS curator $ jcJobs .= mempty
interruptAllJobs c@(getJobCurator -> curator) (WithTimeout delay onTimeout) = do
    interruptAllJobs c Plain
    void $ liftIO . forkIO $ do
        threadDelay delay
        done <- HM.null . view jcJobs <$> readTVarIO curator
        unless done $ liftIO onTimeout >> interruptAllJobs c Force

-- | Waits for this manager to get closed and all registered jobs to invoke
-- `MaskForJobFinished`.
awaitAllJobs :: MonadIO m => JobCurator -> m ()
awaitAllJobs (getJobCurator -> jc) =
    liftIO . atomically $
        check =<< (view jcIsClosed &&^ (HM.null . view jcJobs)) <$> readTVar jc

-- | Interrupts and then awaits for all jobs to complete.
stopAllJobs :: MonadIO m => JobCurator -> m ()
stopAllJobs c = interruptAllJobs c Plain >> awaitAllJobs c

-- | Add second manager as a job to first manager.
addManagerAsJob :: (MonadIO m, MonadTimed m, MonadBaseControl IO m)
                => JobCurator -> InterruptType -> JobCurator -> m ()
addManagerAsJob curator intType managerJob = do
    interrupter <- inCurrentContext $ interruptAllJobs managerJob intType
    addJob curator (JobInterrupter interrupter) $
        \(runMarker -> ready) -> fork_ $ awaitAllJobs managerJob >> liftIO ready

-- | Adds job executing in another thread, where interrupting kills the thread.
addThreadJobGeneral
  :: forall m .
     (CanLog m, HasLoggerName m, MonadIO m,  MonadMask m, MonadTimed m, MonadBaseControl IO m)
  => (m () -> m ()) -> JobCurator -> m () -> m ()
addThreadJobGeneral howToFork curator action = howToFork $ do
    tid <- myThreadId
    -- This thread's ThreadId is *not* retained by the job manager.
    -- That's important. A GHC 'ThreadId' contains a pointer to the
    -- thread itself. If we retain the 'ThreadId', the thread is
    -- retained as well, and will also never receive blocked
    -- indefinitely exceptions, meaning deadlock in our system could
    -- manifest in, for instance, a massive and growing heap.
    wtid <- mkWeakThreadId tid
    killer <- inCurrentContext $ do
        mtid <- liftIO . Weak.deRefWeak $ wtid
        case mtid of
            Just tid -> do
                logInfo $ sformat ("JobCurator : interrupting thread with id " % string) (showThreadId (Proxy :: Proxy m) tid)
                killThread tid
                logInfo $ sformat ("JobCurator : interrupted thread with id " % string) (showThreadId (Proxy :: Proxy m) tid)
            Nothing -> do
                logInfo $ "JobCurator : interrupting dead thread is no-op"
                pure ()
    addJob curator (JobInterrupter killer) $
        \(runMarker -> markReady) -> action `finally` liftIO markReady

addThreadJob
  :: (CanLog m, HasLoggerName m, MonadIO m,  MonadMask m, MonadTimed m, MonadBaseControl IO m)
  => JobCurator -> m () -> m ()
addThreadJob = addThreadJobGeneral fork_

addThreadJobLabeled
  :: (CanLog m, HasLoggerName m, MonadIO m,  MonadMask m, MonadTimed m, MonadBaseControl IO m)
  => String -> JobCurator -> m () -> m ()
addThreadJobLabeled label = addThreadJobGeneral (forkLabeled_ label)

-- | Adds job executing in another thread, interrupting does nothing.
-- Usefull then work stops itself on interrupt, and we just need to wait till it fully
-- stops.
addSafeThreadJob :: (MonadIO m,  MonadMask m, MonadTimed m) => JobCurator -> m () -> m ()
addSafeThreadJob curator action = fork_ $ addJob curator (JobInterrupter $ return ()) $
    \(runMarker -> markReady) -> action `finally` liftIO markReady

isInterrupted :: MonadIO m => JobCurator -> m Bool
isInterrupted = liftIO . atomically . fmap (view jcIsClosed) . readTVar . getJobCurator

unlessInterrupted :: MonadIO m => JobCurator -> m () -> m ()
unlessInterrupted c a = isInterrupted c >>= flip unless a
