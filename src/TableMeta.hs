module TableMeta where

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

-- "dataType" : {
--          "type" : "VARCHAR",
--          "unsigned" : {
--             "none" : true
--          },
--          "len" : {
--             "some" : 64
--          }
--       },
instance ToJSON DataType where
  toJSON DataType{..} = object $
    [ "type" .= t
    , "len" .= maybe (object $ [ "none" .= True ]) (\l -> object $ [ "some" .= l ]) len
    , "unsigned" .= if unsigned then
                      (object $ [ "some" .= ("UNSIGNED" :: String)])
                    else
                      (object $ [ "none" .= True ])
    ]
instance ToJSON Col where
  toJSON Col{..} = object $
    [ "id" .= idNum
    , "name" .= name
    , "dataType" .= (toJSON dataType)
    -- , "pk" .= pk
    , "autoIncrement" .= autoIncrement
    , "nullAble" .= null
    , "defaultValue" .= maybe Null (\x -> object $ maybe [ "none" .= True ] (\j -> [ "some" .= toJSON j ]) x) defaultValue
    , "updateDefaultValue" .= maybe Null toJSON updateDefaultValue
    , "comment" .= maybe Null toJSON comment
    ]

instance ToJSON Index where
  toJSON Index{..} = object $
    [ "id" .= idNum
    , "name" .= name
    , "columns" .= columns
    , "pk" .= pk
    , "comment" .= maybe Null toJSON comment]
instance ToJSON CreateTable

readTableStructure :: MySQLConn -> IO [(Integer, Integer, Text)]
readTableStructure conn = do
  (_, is0)  <- query_ conn "select RelativeID from TableMeta where SchemaID = 253"
  rlst <- Streams.toList is0
  let r_ids = rlst >>= return . Prelude.head >>= return . getInt
  (defs, is) <- query_ conn "SELECT RelativeID, Version, DDL from TableVersionMeta WHERE RelativeID >= 5355"
  lst <- Streams.toList is
  let lst' = lst >>= \row ->
                       let [a, b, c] = row in
                         return (getInt a, getInt b, getText c)
  return $ Prelude.filter (\(a, _, _) -> not $ a `elem` r_ids) lst'
  where
    getInt :: MySQLValue -> Integer
    getInt (MySQLInt16 v) = toInteger v
    getInt (MySQLInt32 v) = toInteger v
    getInt (MySQLInt16U v) = toInteger v
    getInt (MySQLInt32U v) = toInteger v
    getInt v = error $ "cannot getInt from " ++ show v
    getText :: MySQLValue -> Text
    getText (MySQLText v) = v
    getText v = error $ "cannot getText " ++ show v

m :: IO ()
m = do
  conn <- connect defaultConnectInfo
      { ciHost = "192.168.88.103"
      , ciUser = "test"
      , ciPassword = "test"
      , ciDatabase = "old_console" }
  result <- readTableStructure conn
  forM_ result $
     \(a, b, c) -> do
       let ddl= (unpack c) ++ ";"
       putStrLn $ show (a, b)
--       putStrLn $ ddl
       case parse a_tables (show (a, b)) ddl of
         Left err -> error $ show err
         Right result -> do
           let (x, y) = transMeta (Prelude.head result)
           case y of
             Just ct -> (putStrLn $ show (a, x, ct)) >> updateMeta conn a ct
             Nothing -> updateMeta conn a x
  where
    transMeta :: CreateTable -> (CreateTable, Maybe CreateTable)
    transMeta ct@CreateTable{..} =
      let pk' = DL.find (\Col{..} -> pk) columns
      in maybe (ct, Nothing) (\Col{..} ->
                                 let pkIndex = Index { idNum = 1 , name = "PK" , columns = [name] , unique = True , pk = True , comment = Nothing }
                                     indices' = DL.map (\idx@Index{..} -> idx { idNum = 1 + idNum } :: Index) indices
                                in (ct, Just ct { indices = pkIndex:indices' })) pk'
    updateMeta :: MySQLConn -> Integer -> CreateTable -> IO ()
    updateMeta conn rid CreateTable{..} = do
      let columnStr = encode columns
          indexStr = encode indices
          lastColID = case DL.map (\Col{..} -> idNum) columns of
            [] -> 0
            l -> DL.maximum l
          lastIndexID = case DL.map (\Index{..} -> idNum) indices of
            [] -> 0
            l -> DL.maximum l
          qs = "UPDATE TableVersionMeta SET Columns = '" <>
               columnStr <> "', Indices = '" <>
               indexStr <> "', LastColumnId = " <>
               (BS.pack $ show lastColID) <> ", LastIndexId = " <>
               (BS.pack $ show lastIndexID) <> ", Version = 1 WHERE RelativeID = " <> (BS.pack $ show rid)
      ok <- execute_ conn (Query qs)
      putStrLn $ show ok
