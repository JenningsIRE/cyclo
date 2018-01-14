
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

primes :: [Integer]
primes = primes' (2:[3,5..])
  where
    primes' (x:xs) = x : primes' (filter (notDivisorOf x) xs)
    notDivisorOf d n = n `mod` d /= 0

factors :: [Integer] -> Integer -> [Integer]
factors qs@(p:ps) n
    | n <= 1 = []
    | m == 0 = p : factors qs d
    | otherwise = factors ps n
  where
    (d,m) = n `divMod` p

primeFactors :: Integer -> [Integer]
primeFactors = factors primes

numPrimeFactors :: Integer -> Integer
numPrimeFactors = fromIntegral . length . primeFactors

doWork :: Integer -> Integer
doWork = numPrimeFactors

worker :: ( ProcessId, ProcessId) -> Process ()
worker (manager, workQueue) = do
    us <- getSelfPid              -- get our process identifier
    liftIO $ putStrLn $ "Starting worker: " ++ show us
    go us
  where
    go :: ProcessId -> Process ()
    go us = do
      send workQueue us
      receiveWait
        [ match $ \n  -> do
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] given work: " ++ show n

            send manager (doWork n)
            liftIO $ putStrLn $ "[Node " ++ (show us) ++ "] finished work."
            go us
        , match $ \ () -> do
            liftIO $ putStrLn $ "Terminating node: " ++ show us
            return ()
        ]

remotable ['worker]

manager :: Integer    -- The number range we wish to generate work for (there will be n work packages)
        -> [NodeId]   -- The set of cloud haskell nodes we will initalise as workers
        -> Process Integer
manager n workers = do
  us <- getSelfPid

  -- first, we create a thread that generates the work tasks in response to workers
  -- requesting work.
  workQueue <- spawnLocal $ do
    -- Return the next bit of work to be done
    forM_ [1 .. n] $ \m -> do
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
  sumIntegers (fromIntegral n)

-- note how this function works: initialised with n, the number range we started the program with, it calls itself
-- recursively, decrementing the integer passed until it finally returns the accumulated value in go:acc. Thus, it will
-- be called n times, consuming n messages from the message queue, corresponding to the n messages sent by workers to
-- the manager message queue.
sumIntegers :: Int -> Process Integer
sumIntegers = go 0
  where
    go :: Integer -> Int -> Process Integer
    go !acc 0 = return acc
    go !acc n = do
      m <- expect
      go (acc + m) (n - 1)

rtable :: RemoteTable
rtable = Lib.__remoteTable initRemoteTable

-- | This is the entrypoint for the program. We deal with program arguments and launch up the cloud haskell code from
-- here.
someFunc :: IO ()
someFunc = do


  args <- getArgs

  case args of
    ["manager", host, port, n] -> do
      putStrLn "Starting Node as Manager"
      backend <- initializeBackend host port rtable
      startMaster backend $ \workers -> do
        result <- manager (read n) workers
        liftIO $ print result
    ["worker", host, port] -> do
      putStrLn "Starting Node as Worker"
      backend <- initializeBackend host port rtable
      startSlave backend
    _ -> putStrLn "Bad parameters"


  -- create a cloudhaskell node, which must be initialised with a network transport
  -- Right transport <- createTransport "127.0.0.1" "10501" defaultTCPParameters
  -- node <- newLocalNode transport initRemoteTable

  -- runProcess node $ do
  --   us <- getSelfNode
  --   _ <- spawnLocal $ sampleTask (1 :: Int, "using spawnLocal")
  --   pid <- spawn us $ $(mkClosure 'sampleTask) (1 :: Int, "using spawn")
  --   liftIO $ threadDelay 2000000
