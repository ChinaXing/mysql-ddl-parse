{-# LANGUAGE RecordWildCards, DeriveGeneric, OverloadedStrings, DuplicateRecordFields #-}
module Util where

import Lib;
import Model
import Text.Parsec;
import Database.MySQL.Base
import qualified System.IO.Streams as Streams
import Data.Text as DT hiding(filter, map, all, head, zipWith)
import Data.Text.Format
import qualified Data.Text.Lazy as DTL
import Data.Text.Encoding as DE
import qualified Data.Text.IO as TIO
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.List as DL
import TextShow
import Data.Maybe
import Data.Monoid
import Control.Exception
import Data.String
import qualified Data.HashMap as HM
import Control.Monad
import Data.Aeson
import GHC.Generics

instance ToJSON DataType where
  toJSON DataType{..} = object $
    [ "type" .= t
    , "len" .= maybe (object $ [ "none" .= True ]) (\l -> object $ [ "some" .= l ]) len
    , "unsigned" .= if unsigned then
                      (object $ [ "some" .= ("UNSIGNED" :: String)])
                    else
                      (object $ [ "none" .= True ])
    ]

instance FromJSON DataType where
  parseJSON = withObject "dataType" $ \v -> do
    t <- v .: "type"
    l <- v .: "len"
    len <- l .:? "some"
    s <- v .: "unsigned"
    unsigned <- fmap not $ s .:? "none" .!= False
    return DataType{..}


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

instance FromJSON Col where
  parseJSON = withObject "Col" $ \v -> do
    idNum <- v .: "id"
    name <- v .: "name"
    dataType <- v .: "dataType" >>= parseJSON
    autoIncrement <- v .: "autoIncrement"
    null <- v .: "nullAble"
    dv <- v .:? "defaultValue"
    defaultValue <- maybe (return Nothing)
                          (\x ->  fmap Just $ x .:? "some") dv
    updateDefaultValue <- v .:? "updateDefaultValue"
    comment <- v .:? "comment"
    let pk = False
        --defaultValue = Nothing
        --updateDefaultValue = Nothing
        -- comment = Just "OK"
    return Col{..}

instance ToJSON Index where
  toJSON Index{..} = object $
    [ "id" .= idNum
    , "name" .= name
    , "columns" .= columns
    , "pk" .= pk
    , "unique" .= unique
    , "comment" .= maybe Null toJSON comment]

instance FromJSON Index where
  parseJSON = withObject "index" $ \v -> do
    idNum <- v .: "id"
    name <- v .: "name"
    columns <- v .: "columns"
    pk <- v .: "pk"
    unique <- v .:? "unique" .!= False
    comment <- v .:? "comment"
    return Index{..}

eitherError = either error Prelude.id

eitherErrorS :: (Show e) => Either e b -> b
eitherErrorS = either (error . show) Prelude.id

getIntOrNull :: MySQLValue -> Maybe Integer
getIntOrNull MySQLNull = Nothing
getIntOrNull (MySQLInt16 v) = Just $ toInteger v
getIntOrNull (MySQLInt32 v) = Just $ toInteger v
getIntOrNull (MySQLInt16U v) = Just $ toInteger v
getIntOrNull (MySQLInt32U v) = Just $ toInteger v
getIntOrNull v = error $ "cannot getInt from " ++ show v

getInt = fromJust . getIntOrNull

getText :: MySQLValue -> Text
getText (MySQLText v) = v
getText (MySQLBytes v) = decodeUtf8 v
getText v = error $ "cannot getText " ++ show v

-- ad hoc ploymorphism
class MySQLv t where
  toMySQLValue :: t -> MySQLValue

instance MySQLv Integer where
  toMySQLValue = MySQLInt32 . fromInteger

instance MySQLv Int where
  toMySQLValue = MySQLInt32 . fromIntegral

instance MySQLv Text where
  toMySQLValue = MySQLText

get_conn :: Text -> IO MySQLConn
get_conn db = connect defaultConnectInfo
      { -- ciHost = "10.9.119.247"
        ciHost = "192.168.88.103"
      --, ciUser = "shiva"
      , ciUser = "test"
      -- , ciPassword = "n4sd9m6W9i29P"
      , ciPassword = "test"
      , ciDatabase = encodeUtf8 db }

--new_conn = get_conn "shiva_console"
new_conn = get_conn "new_console"
qa_conn = get_conn "qa_console"
prod_conn = get_conn "prod_console"

query_rows :: MySQLConn -> Query -> IO [[MySQLValue]]
query_rows conn sql = do
  (defs, is) <- query_ conn sql
  Streams.toList is

query_rows_stmt :: MySQLConn -> StmtID -> [MySQLValue] -> IO [[MySQLValue]]
query_rows_stmt conn ps v = do
  (defs, is) <- queryStmt conn ps v
  Streams.toList is

query_single_stmt :: MySQLConn -> StmtID -> [MySQLValue] -> IO MySQLValue
query_single_stmt conn ps v = query_rows_stmt conn ps v >>= return . head . head

query_single_row_stmt :: MySQLConn -> StmtID -> [MySQLValue] -> IO [MySQLValue]
query_single_row_stmt conn ps v = query_rows_stmt conn ps v >>= return . head

get_table_meta :: Integer -> Integer -> IO ([Col], [Index], Integer, Integer)
get_table_meta rid version = do
  putStrLn $ " meta -> " ++ show rid ++ " > " ++ show version
  co <- new_conn
  ps <- prepareStmt co "SELECT Columns, Indices, LastColumnId, LastIndexId \
                       \ FROM TableVersionMeta WHERE RelativeID = ? AND Version = ?"
  row <- query_single_row_stmt co ps $ fmap toMySQLValue [rid, version]
  close co
  let [a, b, c, d] = row
      (cols, indices, lcid, liid) = (getText a, getText b, getInt c, getInt d)
      cols' = eitherError $ Util.decode' cols
      indices' = eitherError $ Util.decode' indices
--  putStrLn $ unpack cols
--  putStrLn $ show (eitherDecode $ fromStrict $ encodeUtf8 $ cols :: Either String [Col])
  return (cols', indices', lcid, liid)


get_shard_config :: Text -> Either ParseError ShardConfig
get_shard_config cfg = parse a_shardconfig "" cfg

parse_create_table :: Text -> Either ParseError ([Col], [Index])
parse_create_table struct =
  let x = parse a_createtable "" struct
  in fmap (\CreateTable{..} -> (columns, indices)) x

is_dev_schema_shard :: Integer -> IO Bool
is_dev_schema_shard schema_id = do
  c <- new_conn
  ps <- prepareStmt c "SELECT Shard FROM new_console.SchemaMeta WHERE ID = ?"
  shard <- query_single_stmt c ps [toMySQLValue schema_id]
  close c
  return $ 1 == getInt shard

get_schema_info :: MySQLConn -> Integer -> IO SchemaInfo
get_schema_info c schema_id = do
  ps <- prepareStmt c "SELECT Name, Accounts, Shard, Products FROM SchemaMeta WHERE ID = ? "
  row <- query_single_row_stmt c ps [toMySQLValue schema_id]
  let [n, a, s, p] = row
      schemaName = getText n
      accountIds = read . unpack . getText $ a :: [Integer]
      products = eitherError . Util.decode' . getText $ p :: [Production]
      shard = if getInt s == 0 then False else True
  ps <- prepareStmt c "SELECT UserName FROM SchemaAccount WHERE ID = ?"
  userName <- query_single_stmt c ps [toMySQLValue . head $ accountIds]
  let schemaUser = getText userName
  ps <- prepareStmt c "SELECT ShardCnt, DbUser, RwPolicy, \
                      \ ShardConnPoolSize, ShardMaxConn, MaxConnPerInstance \
                      \ FROM Shards WHERE SchemaID = ?"
  row <- query_single_row_stmt c ps [toMySQLValue schema_id]
  let [sc, du, rp, scps, smc, mcpi] = row
      shardCount = getInt sc
      dbUser = getText du
      rwPolicy = getText rp
      shardConnPoolSize = getInt scps
      shardMaxConn = getInt smc
      maxConnPerInstance = getInt mcpi
  let instanceGroupId = 0 -- if shard then 7 else 6
      clusterId = 0 -- if shard then 3 else 2
      syncSchemaId = 0
  let title = "rds-autoCreate Schema " <> schemaName
  return SchemaInfo{..}

encode' :: (ToJSON a) => a -> Text
encode' a = decodeUtf8 . toStrict $ Data.Aeson.encode a

decode' :: (FromJSON a) => Text -> Either String a
decode' t = Data.Aeson.eitherDecode . fromStrict $ encodeUtf8 t

get_table_versions :: IO MySQLConn -> Integer -> IO [(Integer, [Col], [Index], Integer, Integer)]
get_table_versions conn rid = do
   bracket conn close $ \c -> do
     ps <- prepareStmt c "SELECT Version, Columns, Indices, LastColumnId, LastIndexId \
                         \ FROM TableVersionMeta WHERE RelativeID = ?"
     rows <- query_rows_stmt c ps [toMySQLValue rid]
     return $ DL.map (\[a, b, c, d, e] -> (getInt a
                                          , eitherError $ Util.decode' $ getText b
                                          , eitherError $ Util.decode' $ getText c
                                          , getInt d
                                          , getInt e)) rows


add_table_version ::
  IO MySQLConn -> Integer -> (Integer, [Col], [Index], Int, Int, Text) -> IO OK
add_table_version conn rid (version, cols, indices, lcid, liid, structure) = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "INSERT INTO TableVersionMeta(Version, Columns, Indices, \
                        \ LastColumnId, LastIndexId, RelativeID, DDL) \
                        \ VALUES(?,?,?,?,?,?,?) "
    executeStmt c ps [toMySQLValue version
                     ,toMySQLValue $ encode' cols
                     ,toMySQLValue $ encode' indices
                     ,toMySQLValue lcid
                     ,toMySQLValue liid
                     ,toMySQLValue rid
                     ,toMySQLValue structure]

prepend_table_version :: IO MySQLConn -> Integer -> Text -> IO OK
prepend_table_version conn rid struct = do
  table_versions <- get_table_versions conn rid
  let (cols', indices') = eitherErrorS $ parse_create_table struct
      cols = DL.map (\c@Col{..} -> c { idNum = 0 } :: Col) cols'
      indices = DL.map (\i@Index{..} -> i { idNum = 0 } :: Index) indices'
  let first_version = build_first_table_version table_versions cols indices struct
      (_, _, _, lcid, liid, _) = first_version
  incr_table_version_meta conn rid lcid liid
  add_table_version conn rid first_version

incr_table_version_meta :: IO MySQLConn -> Integer -> Int -> Int -> IO OK
incr_table_version_meta conn rid lcid liid = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "UPDATE TableVersionMeta SET Version = Version + 1, LastColumnId = ?, LastIndexId = ? \
                        \ WHERE RelativeID = ? ORDER BY Version DESC"
    executeStmt c ps [toMySQLValue lcid, toMySQLValue liid, toMySQLValue rid]

build_first_table_version ::
  [(Integer, [Col], [Index], Integer, Integer)] ->
    [Col] ->  [Index]-> Text -> (Integer, [Col], [Index], Int, Int, Text)
build_first_table_version versions cols indices structure = (1, cols', indices', lcid, liid, structure)
  where
    c_name_2_id = DL.foldl (\mp (_, cols0, _, _, _) ->
                              DL.foldl (\m Col{..} -> HM.insert name idNum m) mp cols0)
                    HM.empty versions
    i_name_2_id = DL.foldl (\mp (_, _, idx0, _, _) ->
                              DL.foldl (\m Index{..} -> HM.insert name idNum m) mp idx0)
                    HM.empty versions
    cols0 = DL.map (\c@Col{..} -> maybe c (\id0 -> c { idNum = id0 } :: Col) $
                     (HM.lookup name c_name_2_id)) cols
    idxs0 = DL.map (\i@Index{..} -> maybe i (\id0 -> i { idNum = id0 } :: Index) $
                     (HM.lookup name i_name_2_id)) indices
    c_id_max = HM.fold max 0 c_name_2_id
    i_id_max = HM.fold max 0 i_name_2_id
    (lcid, cols') = DL.foldl (\(mid, r) c@Col{..} ->
                                 if idNum == 0 then (mid + 1, (c { idNum = mid + 1 } :: Col):r)
                                 else (mid, c:r)) (c_id_max, []) cols0
    (liid, indices') = DL.foldl (\(mid, r) i@Index{..} ->
                                    if idNum == 0 then (mid + 1, (i {idNum = mid + 1} :: Index):r)
                                    else (mid, i:r)) (i_id_max, []) idxs0

incr_env_version :: IO MySQLConn -> Integer -> Text -> IO OK
incr_env_version conn rid env = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "UPDATE TableMeta SET Version = Version + 1 WHERE RelativeID = ? AND Env = ?"
    executeStmt c ps [toMySQLValue rid, toMySQLValue env]

--
-- 1. increase version -> version + 1
-- 2. add a track with version equals to 100 to the end , it's a place holder for shift
-- 3. get all version tracks
-- 4. do shift by update fields of records, update in reverse order of id
prepend_version_track :: IO MySQLConn -> Integer -> Maybe [(Integer, Integer)] -> IO [(Integer, Integer)]
prepend_version_track conn tid shifts0 = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "UPDATE TableVersionTrack SET Version = Version + 1 WHERE TableID = ? ORDER BY ID DESC"
    executeStmt c ps [toMySQLValue tid]
  bracket conn close $ \c -> do
    ps <- prepareStmt c "INSERT INTO TableVersionTrack(TableID, \
                        \ SyncID, Version, Operator) VALUES (?,?,?,?)"
    executeStmt c ps [ toMySQLValue tid
                     , toMySQLValue (0 :: Int)
                     , toMySQLValue (100 :: Int)
                     , toMySQLValue ("陈云星(chenyunxing)" :: Text)]
  env_version_tracks <- bracket conn close $ \c -> do
    ps <- prepareStmt c "SELECT ID, TableID, SyncID, Version, Operator, Created, \
                        \ Updated FROM TableVersionTrack WHERE TableID = ? ORDER BY ID ASC"
    rows <- query_rows_stmt c ps [toMySQLValue tid]
    return $ DL.map (\[a, b, c, d, e, f, g] -> (getInt a, getInt b, getInt c, getInt d, getText e, f, g)) rows
  -- putStrLn $ "env version tracks : " ++ (show env_version_tracks)
  let (_, _, _, _, e, f, g) = DL.head env_version_tracks
      (a, b, _, _, _, _, _) = (DL.last env_version_tracks)
      fst_v_sync_id = maybe 0 (fst . head)  shifts0
      fst_v' = (a, b, 0, 1, e, f, g) -- first version syncId = 0 or pre_env's id, version = 1
      -- (V0, V1), (V1, V2), (Vn-1, Vn)
      pairs = DL.zip (fst_v':env_version_tracks) env_version_tracks
      -- (V1, V2), (V2, V3), (Vn-1, Vn)
      shifts' = DL.tail $ DL.map (\((a0, _, _, _, _, _, _), (b0, _, _, _, _, _, _)) -> (a0, b0)) pairs
      shifts = maybe shifts' (++ shifts') shifts0
      -- {V1.id, V0.content, syncID = (adjust with shifts)}
      versions' = DL.map (\((a0, a1, a2, a3, a4, a5, a6), (id0, _, _, _, _, _, _)) ->
                             let sid = if a2 == 0 then a2 else snd . fromJust . DL.find ((==a2) . fst) $ shifts
                             in (id0, a1, sid, a3, a4, a5, a6)) pairs

      (v1_id, v1_tid, _, v1_v, v1_op, v1_c, v1_u) = DL.head versions'
      fst_v = (v1_id, v1_tid, fst_v_sync_id, v1_v, v1_op, v1_c, v1_u)
      version_updates = fst_v:(DL.tail versions')
  -- do update
  --putStrLn $ "pairs : " ++ show pairs
  --putStrLn $ "versions': " ++ show versions'
  --putStrLn $ "shifts': " ++ show shifts'
  --putStrLn $ "shifts: " ++ show shifts
  --putStrLn $ "version_updates: " ++ show version_updates
  bracket conn close $ \c -> do
    ps <- prepareStmt c "UPDATE TableVersionTrack SET SyncID = ?, Version = ?, Operator = ?, \
                        \ Created = ?, Updated = ? WHERE ID = ? "
    forM_ version_updates $ \(c0, c1, c2, c3, c4, c5, c6) -> do
      executeStmt c ps [toMySQLValue c2, toMySQLValue c3
                       ,toMySQLValue c4, c5
                       ,c6, toMySQLValue c0]
  return shifts'

incr_table_versionTrack_version conn tid = do
  bracket conn close  $ \c -> do
    ps <- prepareStmt c "UPDATE TableVersionTrack SET Version = Version + 1 WHERE TableID = ? ORDER BY Version DESC"
    executeStmt c ps [toMySQLValue tid]

fixup_version_track_syncId conn tid shifts = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "UPDATE TableVersionTrack SET SyncID = ? WHERE TableID = ? AND SyncID = ?"
    forM_ (DL.reverse shifts) $ \(a, b) -> do
      executeStmt c ps [toMySQLValue b, toMySQLValue tid, toMySQLValue a]

get_env_tid :: IO MySQLConn -> Integer -> Text -> IO Integer
get_env_tid conn rid env = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "SELECT ID FROM TableMeta WHERE RelativeID = ? AND Env = ?"
    id <- query_single_stmt c ps [toMySQLValue rid, toMySQLValue env]
    return . getInt $ id
