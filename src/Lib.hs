
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
   -- liftIO $ callProcess "git" ["clone", "https://github.com/JenningsIRE/file-server-api"]
    go us
  where
    go :: ProcessId -> Process ()
    go us = do
      send workQueue us
      receiveWait
        [ match $ \n  -> do
       --     liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] given work: " ++ show n
            work <- liftIO $ doWork n
            send manager (work)
            go us
        , match $ \ () -> do
            return ()
        ]

remotable ['worker]

manager :: [String] -> [NodeId] -> Process String
manager files workers = do
  us <- getSelfPid
  workQueue <- spawnLocal $ do
    forM_ files $ \m -> do
      pid <- expect   -- await a message from a free worker asking for work
      send pid m     -- send them work
    forever $ do
      pid <- expect
      send pid ()
  forM_ workers $ \ nid -> spawn nid ($(mkClosure 'worker) (us, workQueue))
  sumIntegers $ length files

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

someFunc :: IO ()
someFunc = do

  args <- getArgs
  curr <- getCurrentDirectory
  case args of
    ["manager", host, port, n] -> do
      callProcess "git" ["clone", "https://github.com/JenningsIRE/file-server-api"]
      backend <- initializeBackend host port rtable
      commits <- getCommits "file-server-api"
      start <- getCurrentTime
      startMaster backend $ \workers -> do
        mapM_ (\commit -> do
          liftIO $ readCreateProcess ((proc "git" ["reset", "--hard", commit]){ cwd = Just "file-server-api"}) ""
          files <- liftIO $ getPaths "file-server-api"
          manager files workers) commits
      end <- getCurrentTime
      let time = diffUTCTime end start
      putStrLn $ show time
      callProcess "rm" ["-rf", "file-server-api"]
    ["worker", host, port] -> do
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> putStrLn "Bad parameters"

getPaths :: FilePath -> IO [FilePath]
getPaths dir = do
    names <- getDirectoryContents dir
    let filteredNames = filter (\f -> head f /= '.' && f /= "argon") names
    paths <- forM filteredNames $ \name -> do
        let path = dir </> name
        isDirectory <- doesDirectoryExist path
        if isDirectory
            then getPaths path
            else return [path]
    return (concat paths)

getCommits :: String -> IO [String]
getCommits repo =  do
    (_, Just hout, _, _) <- createProcess(proc "git" $ words ("--git-dir " ++ repo ++ "/.git log --pretty=format:'%H' ")){ std_out = CreatePipe }
    commits <- hGetContents hout
    return $ map strip $ words commits

strip :: [a] -> [a]
strip [] = []
strip [x] = []
strip xs = tail $ init xs
