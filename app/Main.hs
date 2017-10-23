{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Main where

import Lib
import Lib (DataType(..), Index(..), Col(..))
import Text.Parsec;
import Data.Aeson
import qualified Data.List as DL
import Data.Text
import qualified Data.ByteString.Lazy.Char8 as BS
import Data.ByteString.Lazy.UTF8 (toString)
import Data.Maybe
import Data.Either
import Database.MySQL.Base
import Control.Monad
import Data.Monoid
import qualified System.IO.Streams as Streams

main :: IO ()
main = do
  return ()
