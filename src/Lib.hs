
{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}
--{-# CPP #-}
module Lib ( someFunc ) where

import           Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import           Control.Distributed.Process.Closure
import           Control.Distributed.Process.Node                   (initRemoteTable)
import           Control.Monad
import           Network.Transport.TCP                              (createTransport,
                                                                     defaultTCPParameters)
import           System.Environment                                 (getArgs)
import           System.Exit
import           System.Process
import           System.IO
import           Data.List.Split
import           System.Process
import           System.FilePath ((</>), takeExtension)
import           System.Directory (doesDirectoryExist, getDirectoryContents, getCurrentDirectory)
import           Data.Time.Clock

doWork ::  String -> IO String
doWork n = readProcess "argon" [n] ""


worker :: ( ProcessId, ProcessId) -> Process ()
worker (manager, workQueue) = do
    us <- getSelfPid              -- get our process identifier
    liftIO $ putStrLn $ "Starting worker: " ++ show us
   -- liftIO $ callProcess "git" ["clone", "https://github.com/JenningsIRE/file-server-api"]
    go us
  where
    go :: ProcessId -> Process ()
    go us = do
      send workQueue us
      receiveWait
        [ match $ \n  -> do
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] given work: " ++ show n
            work <- liftIO $ doWork n
            send manager (work)
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] finished work."
            go us
        , match $ \ () -> do
            liftIO $ putStrLn $ "Terminating node: " ++ show us
            return ()
        ]

remotable ['worker]

manager :: [String]    -- The number range we wish to generate work for (there will be n work packages)
        -> [NodeId]   -- The set of cloud haskell nodes we will initalise as workers
        -> Process String
manager files workers = do
  us <- getSelfPid

  -- first, we create a thread that generates the work tasks in response to workers
  -- requesting work.
  workQueue <- spawnLocal $ do
    -- Return the next bit of work to be done
    forM_ files $ \m -> do
      pid <- expect   -- await a message from a free worker asking for work
      send pid m     -- send them work

    -- Once all the work is done tell the workers to terminate. We do this by sending every worker who sends a message
    -- to us a null content: () . We do this only after we have distributed all the work in the forM_ loop above. Note
    -- the indentiation - this is part of the workQueue do block.
    forever $ do
      pid <- expect
      send pid ()

  -- Next, start worker processes on the given cloud haskell nodes. These will start
  -- asking for work from the workQueue thread immediately.
  forM_ workers $ \ nid -> spawn nid ($(mkClosure 'worker) (us, workQueue))
  liftIO $ putStrLn $ "[Manager] Workers spawned"
  -- wait for all the results from the workers and return the sum total. Look at the implementation, whcih is not simply
  -- summing integer values, but instead is expecting results from workers.
  sumIntegers $ length files

-- note how this function works: initialised with n, the number range we started the program with, it calls itself
-- recursively, decrementing the integer passed until it finally returns the accumulated value in go:acc. Thus, it will
-- be called n times, consuming n messages from the message queue, corresponding to the n messages sent by workers to
-- the manager message queue.
sumIntegers :: Int -> Process String
sumIntegers = go ""
  where
    go :: String -> Int -> Process String
    go x 0 = return x
    go x n = do
      m <- expect
      go (x ++ m ++ " ") (n - 1)

rtable :: RemoteTable
rtable = Lib.__remoteTable initRemoteTable

-- | This is the entrypoint for the program. We deal with program arguments and launch up the cloud haskell code from
-- here.
someFunc :: IO ()
someFunc = do


  args <- getArgs
  curr <- getCurrentDirectory
  case args of
    ["manager", host, port, n] -> do
      putStrLn "Starting Node as Manager"
      callProcess "git" ["clone", "https://github.com/JenningsIRE/file-server-api"]
      files <- liftIO $ getFilePaths "file-server-api"
      backend <- initializeBackend host port rtable
      start <- getCurrentTime
      startMaster backend $ \workers -> do
        result <- manager files workers
        liftIO $ putStrLn result
      end <- getCurrentTime
      let time = diffUTCTime end start
      putStrLn $ show time
      callProcess "rm" ["-rf", "file-server-api"]
    ["worker", host, port] -> do
      putStrLn "Starting Node as Worker"
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> putStrLn "Bad parameters"

-- Get Files
getFilePaths :: FilePath -> IO [FilePath]
getFilePaths topdir = do
    names <- getDirectoryContents topdir
    let properNames = filter (\f -> head f /= '.' && f /= "argon") names
    paths <- forM properNames $ \name -> do
        let path = topdir </> name
        isDirectory <- doesDirectoryExist path
        if isDirectory
            then getFilePaths path
            else return [path]
    return (concat paths)
  -- create a cloudhaskell node, which must be initialised with a network transport
  -- Right transport <- createTransport "127.0.0.1" "10501" defaultTCPParameters
  -- node <- newLocalNode transport initRemoteTable

  -- runProcess node $ do
  --   us <- getSelfNode
  --   _ <- spawnLocal $ sampleTask (1 :: Int, "using spawnLocal")
  --   pid <- spawn us $ $(mkClosure 'sampleTask) (1 :: Int, "using spawn")
  --   liftIO $ threadDelay 2000000
