{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings, RecordWildCards, DuplicateRecordFields #-}

module Migration where

import Util as U
import Lib
import Model
import DDL hiding (toJSON)

import Database.MySQL.Base
import qualified System.IO.Streams as Streams
import Data.Text as DT hiding(filter, map, all, head, zipWith)
import Data.Text.Format
import qualified Data.Text.Lazy as DTL
import Data.Text.Encoding as DE
import qualified Data.Text.IO as TIO
import Data.ByteString.Lazy (fromStrict, toStrict)
import TextShow
import qualified Data.List as DL
import Data.Maybe
import Data.Monoid
import Data.String
import Data.Aeson
import qualified Data.List.Split as S
import Control.Monad
import Control.Concurrent
import System.IO

cookie :: Text
cookie = "LambdaKnight=PeVOY9iIMXERWZAUwZJpfhE9aDYzWZ-rqC1hOFv9"

meta_table_list = [ ("Cluster", 10)
                  , ("ClusterConfig", 1500)
                  , ("Proxy", 20)
                  , ("ProxyServerConfig",20)
                  , ("InstanceGroup", 20)
                  , ("HaGroup", 20)
                  , ("Shards", 500)
                  , ("Shard", 20000)
                  , ("SchemaMeta", 500)
                  , ("TableRule",500)
                  , ("TableMeta", 20000)
                  , ("ShardFunction",50)
                  , ("MySQLInstance",20)
                  , ("MySQLHost",20)
                  , ("SchemaAccount", 500)
                  , ("TableVersionMeta", 7000)
                  , ("TableVersionTrack", 9000)]

-- Keep: qa meta data not overlap with dev meta data
modify_qa_with_delta :: IO ()
modify_qa_with_delta = do
  -- for each table, get maxID from new_console , get delta, update ID in qa_console
  new_c <- new_conn
  meta_max_id <- mapM (get_meta_max_id new_c . fst) meta_table_list
  when (not $ all (> 0) meta_max_id) $ error "get max id failure"
  let meta_offset = map snd meta_table_list
  let [ cluster_offset, clusterConfig_offset
        , proxy_offset, proxyServerConfig_offset
        , instanceGroup_offset, haGroup_offset
        , shards_offset, shard_offset
        , schemaMeta_offset, tableRule_offset
        , tableMeta_offset, shardFunction_offset
        , mySQLInstance_offset, mySQLHost_offset
        , schemaAccount_offset, tableVersionMeta_offset
        , tableVersionTrack_offset ] = meta_offset
  let gt0 = all (> 0) $ zipWith (-)  meta_offset meta_max_id
  when (not gt0) $ do
    mapM_ (putStrLn . show) $ zipWith (,) meta_offset meta_max_id
    error "offset not great than Zero, please Fix it !"
  putStrLn "get delta ok ..."
  let fix_SQL =
        [ format "UPDATE Cluster SET ID = ID + {} ORDER BY ID DESC" (Only cluster_offset)
        , format "UPDATE MySQLHost SET ID = ID + {} ORDER BY ID DESC " (Only mySQLHost_offset)
        , format "UPDATE MySQLInstance SET ID = ID + {}, MySQLHostID = MySQLHostID + {}, \
                 \ HaGroupID = HaGroupID + {}, InstanceGroupID = InstanceGroupID + {} ORDER BY ID DESC"
          (mySQLInstance_offset, mySQLHost_offset, haGroup_offset, instanceGroup_offset)
        , format "UPDATE InstanceGroup SET ID = ID + {} ORDER BY ID DESC" (Only instanceGroup_offset)
        , format "UPDATE HaGroup SET ID = ID + {}, InstanceGroupID = InstanceGroupID + {} ORDER BY ID DESC"
          (haGroup_offset, instanceGroup_offset)
        , format "UPDATE SchemaMeta SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (schemaMeta_offset, cluster_offset)
        , format "UPDATE SchemaAccount SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (schemaAccount_offset, cluster_offset)
        , format "UPDATE TableMeta SET ID = ID + {}, SchemaID = SchemaID + {}, \
                 \ ClusterID = ClusterID + {} ORDER BY ID DESC"
          (tableMeta_offset, schemaMeta_offset, cluster_offset)
        , format "UPDATE TableRule SET ID = ID + {}, ClusterID = ClusterID + {}, \
                 \ AlgFunctionId = AlgFunctionId + {} ORDER BY ID DESC"
          (tableRule_offset, cluster_offset, shardFunction_offset)
        , format "UPDATE ShardFunction SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (shardFunction_offset, cluster_offset)
        , format "UPDATE Shard SET ID = ID + {}, ClusterID = ClusterID + {}, \
                 \ HaGroupID = HaGroupID + {}, ShardsID = SHardsID + {} ORDER BY ID DESC"
          (shard_offset, cluster_offset, haGroup_offset, shards_offset)
        , format "UPDATE Shards SET ID = ID + {}, ClusterID = ClusterID + {}, \
                 \ InstanceGroupID = InstanceGroupID + {}, SchemaID = SchemaID + {} ORDER BY ID DESC"
          (shards_offset, cluster_offset, instanceGroup_offset, schemaMeta_offset)
        , format "UPDATE Proxy SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (proxy_offset, cluster_offset)
        , format "UPDATE ProxyServerConfig SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (proxyServerConfig_offset, cluster_offset)
        , format "UPDATE ClusterConfig SET ID = ID + {}, ClusterID = ClusterID + {} ORDER BY ID DESC"
          (clusterConfig_offset, cluster_offset)
        , format "UPDATE new_console.SchemaCorrelation SET QaID = QaID + {}" (Only schemaMeta_offset)
        ]
  qa_c <- qa_conn
  mapM_ (\sql -> do
            TIO.putStrLn sql
            execute_ qa_c . Query . fromStrict . encodeUtf8 $ sql
        ) $ map (DTL.toStrict) fix_SQL
  -- mapM_ (putStrLn . unpack . DTL.toStrict) fix_SQL
  close qa_c
  close new_c
  -- update accounts field of SchemaMeta
  qa_c <- qa_conn
  rows <- query_rows qa_c "SELECT ID, Accounts FROM SchemaMeta"
  ps <- prepareStmt qa_c "UPDATE SchemaMeta SET Accounts = ? WHERE ID = ?"
  mapM_ (\[id, a] -> do
            let accounts = read . unpack . getText $ a :: [Integer]
                accounts' = map (+ schemaAccount_offset) accounts
            executeStmt qa_c ps [toMySQLValue . showt $ accounts', id]
        ) rows
  close qa_c
  where
    get_meta_max_id :: MySQLConn -> Text -> IO Integer
    get_meta_max_id c t = do
      let sql =
            if t == "TableVersionMeta" then
              "SELECT MAX(RelativeID) FROM TableVersionMeta"
            else
              Query $ fromStrict $ encodeUtf8 $ "SELECT MAX(ID) FROM " <> t
      rows <- query_rows c sql
      let v = getIntOrNull $ head $ head rows
      when (isNothing v) $ error $ "cannot get max 'ID' of Table : " <> unpack t
      return $ fromJust v

get_schema_qa_only :: IO [Integer]
get_schema_qa_only = do
  c <- qa_conn
  let sql = "select b.ID from qa_console.SchemaMeta b left join new_console.SchemaMeta a  \
            \ on a.Name = b.Name AND a.Shard = b.Shard \
            \ WHERE a.ID is NULL"
  rows <- query_rows c sql
  return $ map (getInt . head) rows

submit_create_schema :: Integer -> IO (Either Text Integer)
submit_create_schema schema_id = do
  schemaInfo <- get_schema_info schema_id
  submit_ddl_operate cookie
    "http://pre-rds.qima-inc.com/api/ddl/operate/daily/plan/create_schema"
    schemaInfo

-- schema morphis from dev to qa
-- some schema exist in qa but not in dev,
-- print them to stdout and write to a file qa-only.schema
-- some schema exist in dev but no in qa,
-- print them to stdout and write to a file dev-only.schema
set_up_schema_correlation :: IO ()
set_up_schema_correlation = do
  c <- qa_conn
  let sql = "select a.ID as devID , b.ID as qaID from new_console.SchemaMeta a \
            \ left join qa_console.SchemaMeta b on a.Name = b.Name AND a.Shard = b.Shard \
            \ union \
            \select a.ID as devID , b.ID as qaID from qa_console.SchemaMeta b \
            \ left join new_console.SchemaMeta a on a.Name = b.Name AND a.Shard = b.Shard"
  rows <- query_rows c sql
  updatePreStmt <- prepareStmt c "INSERT INTO new_console.SchemaCorrelation(DailyID, QaID, ProdID) VALUES(?,?,0)"
  let co = map get_correlation rows
      qa_only = map (fromJust . snd) $ filter (isNothing . fst) $ co
      dev_only = map (fromJust . fst) $ filter (isNothing . snd) $ co
      both_exist = map (\(x, y) -> (fromJust x, fromJust y)) $
        filter (\(x, y) -> isJust x && isJust y) $ co
  writeFile "./dev-only.schema" (show dev_only)
  writeFile "./qa-only.schema" (show qa_only)
  mapM_ (set_co c updatePreStmt) both_exist
  close c
  where
    get_correlation :: [MySQLValue] -> (Maybe Integer, Maybe Integer)
    get_correlation [a, b] = (getIntOrNull a, getIntOrNull b)
    set_co :: MySQLConn -> StmtID -> (Integer, Integer) -> IO ()
    set_co c updatePreStmt (devID, qaID) = do
      executeStmt c updatePreStmt
        [toMySQLValue devID, toMySQLValue qaID]
      return ()

set_up_table_relative :: IO ()
set_up_table_relative = do
  nc <- new_conn
  schemaPairList <- get_schema_list nc
  close nc
  let schemaLists = S.chunksOf 10 schemaPairList
  mapM_ (\s -> forkIO $ do_set_relative s) schemaLists
  where
    do_set_relative :: [(Integer, Integer)] -> IO ()
    do_set_relative l = do
      nc <- new_conn
      qc <- qa_conn
      ps <- prepareStmt qc "UPDATE TableMeta SET RelativeID = ? WHERE Name = ? AND SchemaID = ? "
      ps1 <- prepareStmt nc "SELECT Name, RelativeID FROM TableMeta WHERE SchemaID = ?"
      mapM_ (\(devId, qaId) -> do
                     tables <- get_schema_tables nc ps1 devId
                     mapM_ (set_qa_table_relativeId qc ps qaId) tables
                 ) l
      close nc
      close qc
    get_schema_list :: MySQLConn -> IO [(Integer, Integer)]
    get_schema_list c = do
      rows <- query_rows c "SELECT DailyID, QaID FROM SchemaCorrelation"
      return $ map (\[x, y] -> (getInt x, getInt y)) rows
    get_schema_tables :: MySQLConn -> StmtID -> Integer -> IO [(Text, Integer)]
    get_schema_tables c ps schemaId = do
      rows <- query_rows_stmt c ps [toMySQLValue schemaId]
      return $ map (\[x, y] -> (getText x, getInt y)) rows
    set_qa_table_relativeId :: MySQLConn -> StmtID -> Integer -> (Text, Integer) -> IO ()
    set_qa_table_relativeId c ps schemaId (table_name, relative_id) = do
--      TIO.putStrLn $ (showt schemaId) <> " > " <> table_name <> " > " <> (showt relative_id)
      executeStmt c ps [toMySQLValue relative_id, MySQLText table_name, toMySQLValue schemaId]
      return ()

set_up_table_version_which_structure_equals :: IO ()
set_up_table_version_which_structure_equals = do
  nc <- new_conn
  qc <- qa_conn
  let sql0 = "UPDATE TableMeta SET Version = ? WHERE ID = ?"
  ps0 <- prepareStmt qc sql0
  let sql1 = "INSERT INTO TableVersionTrack(TableID, SyncID, Version, Operator) VALUES(?, ?, ?, ?)"
  ps1 <- prepareStmt nc sql1
  let sql2 ="SELECT Version FROM TableVersionMeta WHERE DDL = ? AND RelativeID = ?"
  ps2 <- prepareStmt nc sql2
  let sql3 = "SELECT ID FROM TableVersionTrack WHERE TableID = \
             \ (SELECT ID FROM TableMeta WHERE RelativeID = ? ) AND Version = ? LIMIT 1"
  ps3 <- prepareStmt nc sql3
  tables <- get_tables
  mapM_ (set_up_table_version (nc, qc) (ps0, ps1, ps2, ps3)) tables
  close nc
  close qc
  where
    get_tables :: IO [(Integer, Integer, Text)]
    get_tables = do
      c <- qa_conn
      -- 获取所有与Dev存在的表
      rows <- query_rows c "SELECT ID, RelativeID, Structure FROM TableMeta WHERE RelativeID != 0"
      close c
      return $ map (\[x, y, z] -> (getInt x, getInt y, getText z)) rows
    --set_up_table_version :: (MySQLConn, MySQLConn) -> (StmtID, StmtID, StmtID, StmtID) -> (Integer, Integer, Text) -> IO ()
    set_up_table_version (nc, qc) (ps0, ps1, ps2, ps3) (tableID, relativeID, struct) = do
      TIO.putStrLn $ DTL.toStrict $ format "> {} > {}" (tableID, relativeID)
      rows <- query_rows_stmt nc ps2 [MySQLText struct, toMySQLValue relativeID]
      when (Prelude.length rows > 1) $ error ("get more then 1 version : " <> (show relativeID))
      if Prelude.length rows == 1 then
        do
          --TIO.putStrLn $ "Got match : " <> (showt tableID)
          let version = getInt . head . head $ rows
          rows <- query_rows_stmt nc ps3 [toMySQLValue relativeID, toMySQLValue version]
          let syncID = getInt . head . head $ rows
          -- update qa_console tableMeta's version
          executeStmt qc ps0 [toMySQLValue version, toMySQLValue tableID]
          -- add tableVersionTrack
          executeStmt nc ps1 [ toMySQLValue tableID
                             , toMySQLValue syncID
                             , toMySQLValue version
                             , MySQLText "陈云星(chenyunxing)"]
          return ()
        else return ()

--
-- table in qa which cannot find its version in dev
-- 1. make qa older version, forward dev version by one
-- 2. add version track
table_qa_not_match :: IO ()
table_qa_not_match = do
  c <- qa_conn
  rows <- query_rows c "SELECT s.ID, s.Name, t.ID, t.Name FROM TableMeta t \
                       \ left join SchemaMeta s ON t.SchemaID = s.ID \
                       \ WHERE t.RelativeID != 0 AND t.Version = 0"
  let all = map (\[a, b, c, d] -> (getInt a, getText b, getInt c, getText d)) rows
  TIO.writeFile "./qa-not-match.table" (DT.unlines $ map showt all)
  close c

--
-- table exist in qa, but not in dev
--
-- submit create schema plan, await create schema done
-- foreach tables in these schema, just follow the normal flow to process them:
-- 1. set up schema correlation
-- 2. fix_up_table_qa_only
--
table_qa_only :: IO [(Integer, Integer, Text, Text, Integer)]
table_qa_only = do
  c <- qa_conn
  rows <- query_rows c "SELECT s.ID, t.ID, s.Name, t.Name FROM TableMeta t \
                       \ left join SchemaMeta s ON t.SchemaID = s.ID \
                       \ WHERE t.State = 'ONLINE' AND t.RelativeID = 0 AND s.ID IN \
                       \ (SELECT QaID FROM new_console.SchemaCorrelation)"
  let all = map (\[a, b, c, d] -> (getInt a, getInt b, getText c, getText d)) rows
  ps <- prepareStmt c "SELECT DailyID FROM new_console.SchemaCorrelation WHERE QaID = ?"
  all' <- mapM (set_schemaId c ps) all
  TIO.writeFile "./qa-only.table" (DT.unlines $ map showt all')
  close c
  return all'
  where
    set_schemaId :: MySQLConn -> StmtID -> (Integer, Integer, Text, Text) -> IO (Integer, Integer, Text, Text, Integer)
    set_schemaId conn ps (a,b,c,d) = do
      rows <- query_rows_stmt conn ps [toMySQLValue $ a]
      let dev_id = getInt . head . head $ rows
      return (a, b, c, d, dev_id)


--
-- fix up table not exist in dev, but exist in qa
--
-- parse DDL to structure, submit create table plan
-- then, go normal flow to set up relation ship between qa and dev:
-- 1. set_up_table_version_which_structure_equals
--

fix_up_table_qa_only :: IO ()
fix_up_table_qa_only = do
  table_qa_only >>= return . map (\(a, b, c, d, e) -> (e, b)) >>= fix_up_table_qa_only0

fix_up_table_qa_only0 :: [(Integer, Integer)] -> IO () -- (dev_schemaID, tableId)
fix_up_table_qa_only0 tables = do
  let chunks = S.chunksOf 100 $ tables
  planIds <- mapM fix_table chunks
  putStrLn $ show planIds
  return ()

submit_create_table :: Text -> [TableConfig] -> IO (Either Text Integer)
submit_create_table title tableConfig = do
  submit_ddl_operate cookie
    "http://pre-rds.qima-inc.com/api/ddl/operate/daily/plan/create_table"
    CreateTableInfo{..}

fix_table :: [(Integer, Integer)] -> IO Integer
fix_table tables =  do
  putStrLn "enter fix chunk .."
  cfgs <- mapM get_table_create_info tables
  putStrLn $  "sumbit -> " ++ (show . Prelude.length $ cfgs)
  fmap (either (error . unpack) Prelude.id) $ submit_create_table "rds-auto create table dev missing " cfgs

get_table_create_info :: (Integer, Integer) -> IO TableConfig
get_table_create_info (dev_schemaId, tableId) = do
  (tableName, structure, shardRules ) <- get_table_info tableId
  putStr $ show (dev_schemaId, tableId)
  let (columns', indices') = fromJust $ either (const Nothing) Just $ parse_create_table structure
  shard <- is_dev_schema_shard dev_schemaId
  putStrLn $ " parse done " ++ (show . Prelude.length $ columns')
  putStrLn $ " sr : " ++ (unpack shardRules)
  let shardConfig = if shard then
        let shardRules' = head . read $ unpack shardRules :: Text
        in Just (fromJust $ either (const Nothing) Just $ get_shard_config shardRules')
        else Nothing
  putStrLn $ show shardConfig
  let columns = map col_2_col_ columns'
      indices = map index_2_index_ indices'
      schemaId = dev_schemaId
  return TableConfig {..}

--
-- reinit qa_console
--
-- drop database qa_console; create database qa_console ; use qa_console; source qa.sql;
--
-- reinit new_console
--
-- drop database new_console; create database new_console ;use new_console ; source new-console.sql;
--
--
-- delete from qa_console.SchemaMeta where Name = 'risk_manage' AND State = 'INIT';
--
-- alter table qa_console.TableMeta add index SchemaID_Name (SchemaID, Name);
--



delete_cfgs = []


delete_cfgs_group = do
  let cfgs = DL.groupBy (\(x, y) (z, k) -> x == z) delete_cfgs
  let title = "drop table incorrect"
      hdt = HdT { delay =  0, timeMinutes = 1140}
  mapM (\cf -> do
           let tableConfig = map (cdc hdt) cf
           submit_ddl_operate cookie
             "http://pre-rds.qima-inc.com/api/ddl/operate/daily/plan/drop_table"
             DelTableInfo{..}
       ) cfgs
  where
    cdc :: HdT -> (Integer, Integer) -> DtC
    cdc hardDropTime (schemaId, tableId) = DtC{..}

-- parse qa tables , and then compare columns/indices with dev's versions
-- if they equals , table equals, set up version to matched version
-- otherwise, dump to a file , qa-not match, produce later
cmp_structure_and_setup_qa_tables :: IO ()
cmp_structure_and_setup_qa_tables = do
  tables <- get_table_list
  mapM parse_qa_table tables
  return ()

parse_qa_table :: (Integer, Integer, Text, Text) -> IO ()
parse_qa_table (table_id, relative_id, name, struct) = do
  putStrLn $ show table_id ++ " > " ++ show relative_id ++ " > " ++ show name
  let r = parse_create_table struct
  let (columns, indices) = either (\e -> error $ (unpack struct) ++ "\n" ++ show e  ) Prelude.id r
  versions <- get_table_versions relative_id
  let v = DL.find (\(version, vs) -> struct_equals vs columns indices) versions
  maybe (return ()) (\(v9, _) -> set_up_version table_id relative_id v9) v

struct_equals :: Text -> [Col] -> [Index] -> Bool
struct_equals struct columns indices =
  let r = parse_create_table struct
      (cols, indx) = either (\e -> error $ (unpack struct) ++ "\n" ++ show e) Prelude.id r
  in (compareCol cols columns) && (compareIndex indx indices)

compareCol :: [Col] -> [Col] -> Bool
compareCol a b =
  let al = Prelude.length a
      bl = Prelude.length b
  in if al /= bl then False else all (\c -> DL.any (col_equals c) b) a

col_equals :: Col -> Col -> Bool
col_equals c0 c1 =
  let c0' = c0 { idNum  = 0 } :: Col
      c1' = c1 { idNum  = 0 } :: Col
  in c0' == c1'

compareIndex :: [Index] -> [Index] -> Bool
compareIndex a b =
  if Prelude.length a /= Prelude.length b then False else all (\i -> DL.any (index_equals i) b) a

index_equals :: Index -> Index -> Bool
index_equals i0 i1 =
  let i0' = i0 { idNum = 0} :: Index
      i1' = i1 { idNum = 0}  :: Index
  in i0' == i1'

set_up_version :: Integer -> Integer -> Integer -> IO ()
set_up_version table_id relative_id version = do
  putStrLn $ " setup -> " ++ show table_id ++ " > "  ++ show relative_id ++ " > " ++ show version
  c <- new_conn
  ps <- prepareStmt c "SELECT a.ID FROM new_console.TableVersionTrack a left join \
                      \ TableMeta b ON b.ID = a.TableID WHERE b.RelativeID = ? AND a.Version = ?"
  rows <- query_rows_stmt c ps [toMySQLValue relative_id, toMySQLValue version]
  let syncId = getInt . head . head $ rows
  ps <- prepareStmt c "INSERT INTO new_console.TableVersionTrack(TableID, Version, SyncID) VALUES(?,?,?)"
  executeStmt c ps  [toMySQLValue table_id, toMySQLValue version, toMySQLValue syncId]
  ps <- prepareStmt c "UPDATE qa_console.TableMeta SET Version = ? WHERE ID = ?"
  executeStmt c ps [toMySQLValue version, toMySQLValue table_id]
  close c
  return ()

get_table_versions :: Integer -> IO [(Integer, Text)]
get_table_versions relative_id = do
  c <- new_conn
  ps <- prepareStmt c "SELECT Version, DDL FROM new_console.TableVersionMeta WHERE RelativeID = ?"
  rows <- query_rows_stmt c ps [toMySQLValue relative_id]
  close c
  return $ map (\[b, d] -> (getInt b, getText d)) rows

get_table_list :: IO [(Integer, Integer, Text, Text)]
get_table_list = do
  c <- qa_conn
  rows <- query_rows c "SELECT ID, RelativeID, Name, Structure FROM \
                       \ qa_console.TableMeta WHERE RelativeID != 0 AND Version = 0"
  close c
  return $ map (\[a, b, c, d] -> (getInt a, getInt b, getText c, getText d)) rows

----------
---------- set up qa dev not equals
----------

set_up_not_equals :: IO ()
set_up_not_equals = do
  tables <- get_not_equals_table_list
  mapM_ set_up_not_equals_one tables
  return ()

set_up_not_equals_one :: (Integer, Integer, Text, Text) -> IO ()
set_up_not_equals_one (qa_tableId, relativeId, tableName, struct) = do
  let (columns, indices) = either (\e -> error $ show e) Prelude.id $ parse_create_table struct
  -- insert versionMeta
  prepend_version_meta relativeId columns indices struct
  -- prepend versionTrack of dev
  sync_id <- prepend_dev_version_track relativeId
  -- insert versionTrack of qa
  add_qa_version_track qa_tableId sync_id
  -- setup qa tableMeta's Version
  c <- new_conn
  ps <- prepareStmt c "UPDATE qa_console.TableMeta SET Version = 1 WHERE ID = ?"
  executeStmt c ps [toMySQLValue qa_tableId]
  close c
  return ()

add_qa_version_track :: Integer -> Integer -> IO ()
add_qa_version_track qa_tid sync_id = do
  c <- new_conn
  ps <- prepareStmt c "INSERT INTO TableVersionTrack(TableID, Version, SyncID, Operator) VALUES(?,?,?,?)"
  executeStmt c ps [toMySQLValue qa_tid
                   , toMySQLValue (1 :: Integer)
                   , toMySQLValue sync_id
                   , toMySQLValue ("陈云星(chenyunxing)" :: Text)]
  close c
  return ()

prepend_version_meta :: Integer -> [Col] -> [Index] -> Text -> IO ()
prepend_version_meta rid cols0 idxs0 struct = do
  (dev_tid, version, cols, idxs, lcid, liid) <- get_dev_table_columns_indices rid 1
  let (lcid', columns') = assoc_columns_ids lcid cols0 cols
      (liid', indices') = assoc_indices_ids liid idxs0 idxs
  c <- new_conn
  -- update version -> version + 1
  ps <- prepareStmt c "UPDATE TableVersionMeta SET Version = Version + 1 WHERE RelativeID = ? ORDER BY Version DESC"
  executeStmt c ps [toMySQLValue rid]
  -- update dev version = version + 1
  when(dev_tid == 19366) $ putStrLn $ " incr version : " ++ show version
  ps <- prepareStmt c "UPDATE TableMeta SET Version = Version + 1 WHERE ID = ?"
  executeStmt c ps [toMySQLValue dev_tid]
  -- prepend version = 1
  ps <- prepareStmt c "INSERT INTO TableVersionMeta(RelativeID, Columns, Indices, \
                      \ LastColumnId, LastIndexId, Version, DDL, Operator) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
  let operator = "陈云星('chenyunxing')" :: Text
  executeStmt c ps $ [toMySQLValue rid
                      , toMySQLValue . decodeUtf8 . toStrict . encode $ columns'
                      , toMySQLValue . decodeUtf8 . toStrict . encode $ indices'
                     , toMySQLValue lcid'
                     , toMySQLValue liid'
                     , toMySQLValue (1 :: Integer) -- always the first version
                     , toMySQLValue struct
                     , toMySQLValue operator]
  close c
  return ()

prepend_dev_version_track :: Integer -> IO Integer
prepend_dev_version_track rid  = do
  c <- new_conn
  ps <- prepareStmt c "SELECT ID FROM TableMeta WHERE RelativeID = ? AND Env = 'DAILY'"
  rows <- query_rows_stmt c ps [toMySQLValue rid]
  let dev_tid = getInt . head . head $ rows
  ps <- prepareStmt c "INSERT INTO TableVersionTrack(TableID, SyncID, Version, Operator) VALUES(?,?,?,?)"
  executeStmt c ps $ [toMySQLValue dev_tid
                      , toMySQLValue (0 :: Integer)
                      , toMySQLValue (100000 :: Integer)
                      , toMySQLValue ("xx" :: Text)]
  ps <- prepareStmt c "SELECT ID, Version, SyncID, Operator FROM TableVersionTrack WHERE TableID = ? ORDER BY ID ASC"
  rows <- query_rows_stmt c ps [toMySQLValue dev_tid]
  let records = map (\[a, b, c, d] -> (getInt a, getInt b, getInt c, getText d)) rows
  -- zip id pair
  ps <- prepareStmt c "UPDATE TableVersionTrack SET SyncID = ?, Version = ?, Operator = ? \
                      \ WHERE ID = ?"
  let pairs = DL.zip records (DL.tail records)
  mapM_ (\((id0, v0, sid0, op0), (id1, v1, sid1, op1)) -> do
            let sync_id = if sid0 == 0 then sid0 else
                  let (_, (x,_,_,_)) = fromJust $ DL.find (\((c, _, _, _), _) -> c == sid0) pairs
                  in x
            putStrLn $ show dev_tid ++ " move version track : " ++ show id0 ++ " -> " ++ show id1
            executeStmt c ps [toMySQLValue sid0, toMySQLValue (v0 + 1), toMySQLValue op0, toMySQLValue id1]
            ) $ DL.reverse pairs
  -- insert new data to first place
  let (id_, v, sid, op) = head records
  executeStmt c ps [toMySQLValue (0 :: Integer)
                   , toMySQLValue (1 :: Integer)
                   , toMySQLValue op
                   , toMySQLValue id_]
  close c
  return id_

assoc_indices_ids0 :: [Index] -> [Index] -> [Index]
assoc_indices_ids0 n o = do
  i <- n
  return $ case DL.find (\x -> (name (i :: Index)) == (name (x :: Index))) o of
             Nothing -> i {idNum = 0} :: Index
             Just v -> i { idNum = (idNum (v :: Index)) } :: Index

assoc_indices_ids :: Integer -> [Index] -> [Index] -> (Integer, [Index])
assoc_indices_ids liid n o =
  let index' = assoc_indices_ids0 n o
      zero_id_indices = filter (\i -> idNum (i :: Index) == 0) index'
      nextId = liid + 1
      zero_id_indices' = map (\(id, i) -> i { idNum = fromInteger id} :: Index) $ DL.zip [nextId..] zero_id_indices
      no_zero_indices = filter (\i -> idNum (i :: Index) /= 0) index'
  in (liid + (toInteger $ Prelude.length zero_id_indices'), (DL.concat [no_zero_indices, zero_id_indices']))

assoc_columns_ids0 :: [Col] -> [Col] -> [Col]
assoc_columns_ids0 n o = do
  c <- n
  return $ case DL.find (\x -> (name (c :: Col)) == (name (x :: Col))) o of
            Nothing -> c { idNum = 0} :: Col
            Just v -> c { idNum = (idNum (v :: Col)) } :: Col

assoc_columns_ids :: Integer -> [Col] -> [Col] -> (Integer, [Col])
assoc_columns_ids lastId n o =
  let col' = assoc_columns_ids0 n o
      zero_id_cols = filter (\c -> idNum (c :: Col) == 0) col'
      nextId = lastId + 1
      zero_id_cols' = map (\(id, c) -> c { idNum = fromInteger id} :: Col) $ DL.zip [nextId..] zero_id_cols
      no_zero_cols = filter (\c -> idNum (c :: Col) /= 0) col'
  in (lastId + (toInteger $ Prelude.length zero_id_cols'), (DL.concat [no_zero_cols, zero_id_cols']))


get_dev_table_columns_indices :: Integer -> Integer -> IO (Integer, Integer, [Col], [Index], Integer, Integer)
get_dev_table_columns_indices relativeId version = do
  (tid, _) <- get_table_version_by_relative_id relativeId
  (cols, indices, lcid, liid) <- get_table_meta relativeId version
  return (tid, version, cols, indices, lcid, liid)

get_table_version_by_relative_id :: Integer -> IO (Integer, Integer)
get_table_version_by_relative_id rid = do
  c <- new_conn
  ps <- prepareStmt c "SELECT ID, Version FROM TableMeta WHERE RelativeID = ? AND Env = 'DAILY'"
  rows <- query_rows_stmt c ps [toMySQLValue rid]
  close c
  let [a, b] = head $ rows
  return (getInt a, getInt b)

get_not_equals_table_list :: IO [(Integer, Integer, Text, Text)]
get_not_equals_table_list = do
  c <- qa_conn
  rows <- query_rows c "SELECT ID, RelativeID, Name, Structure FROM \
                       \ qa_console.TableMeta WHERE Version = 0 AND RelativeID != 0"
  close c
  return $ map (\[a, b, c, d] -> (getInt a, getInt b, getText c, getText d)) rows


-------
------- fix table uqnie key
-------
fix_table_unique_keys :: IO ()
fix_table_unique_keys = do
  relative_ids <- get_qa_has_unique_key_relative_ids
  changed <- mapM fix_table_unique_key_one relative_ids
  -- for each table generate ALTER Statement, write to file : fix-unique.sql
  let sqls = map (\(x,l) ->
           -- tname <- get_dev_table_name x
                let sqls0 = map (\i ->
                             let i_name = (name :: Index -> String) i
                                 sql = "UPDATE TableVersionMeta SET DDL=REPLACE(DDL, \"KEY `"
                                       ++ i_name ++ "`\", \"UNIQUE KEY `" ++ i_name ++ "`\")"
                                       ++ " WHERE RelativeID = " ++ (show x) ++ ";"
                                 sql_index = "UPDATE TableVersionMeta SET Indices=REPLACE(Indices, \
                                         \ '\"name\":\"" ++ i_name ++ "\",\"pk\":false,\"unique\":false', \
                                         \ '\"name\":\"" ++ i_name ++ "\",\"pk\":false,\"unique\":true') \
                                         \ WHERE RelativeID = " ++ (show x) ++ ";"
                             --putStrLn $ sql
                             --putStrLn $ sql_index
                             in [sql, sql_index]
                         ) l
                in DL.concat sqls0
            ) changed
  writeFile "./fix-unique-key.sql" $ Data.String.unlines $ DL.concat sqls

get_dev_table_name :: Integer -> IO Text
get_dev_table_name rid = do
  nc <- new_conn
  ps <- prepareStmt nc "SELECT Name FROM TableMeta WHERE RelativeID = ?"
  rows <- query_rows_stmt nc ps [toMySQLValue rid]
  close nc
  return $ getText . head . head $ rows

fix_table_unique_key_one :: Integer -> IO (Integer, [Index])
fix_table_unique_key_one rid = do
  dev_keys <- get_dev_table_keys rid
  qa_keys <- get_qa_table_keys rid
  let todo_keys = filter (is_need_fix_unique_key rid qa_keys) dev_keys
  -- fix_unique_key rid dev_keys todo_keys
  return (rid, todo_keys)

fix_unique_key :: Integer -> [Index] -> [Index] -> IO [Index]
fix_unique_key rid dev_keys unique_keys = do
  -- update Indices field of TableVersionMeta
  let nidx = map (\i ->
                case DL.find (\x -> (name :: Index -> String) x == (name :: Index -> String) i) unique_keys of
                  Nothing -> i
                  Just _ -> i { unique = True } :: Index
             ) dev_keys
      s = encode' nidx
  nc <- new_conn
  ps <- prepareStmt nc "UPDATE new_console.TableVersionMeta SET Indices = ? WHERE RelativeID = ?"
  executeStmt nc ps [toMySQLValue s, toMySQLValue rid]
  -- execute UPDATE to replace DDL part
  ps <- prepareStmt nc "UPDATE new_console.TableVersionMeta SET \
                       \ DDL=REPLACE(DDL, 'KEY ?', 'UNIQUE KEY ?') WHERE RelativeID = ?"
  mapM_ (\Index{..} -> do
            executeStmt nc ps [toMySQLValue $ pack name, toMySQLValue $ pack name, toMySQLValue rid]
        ) unique_keys
  close nc
  return unique_keys

get_dev_table_keys :: Integer -> IO [Index]
get_dev_table_keys rid = do
  nc <- new_conn
  ps <- prepareStmt nc "SELECT Indices FROM new_console.TableVersionMeta WHERE RelativeID = ?"
  rows <- query_rows_stmt nc ps [toMySQLValue rid]
  close nc
  when (Prelude.length rows /= 1)
    (error $ "expect 1 row, got : " ++ (show $ Prelude.length rows) ++ "," ++ (show rid))

  let x = eitherDecode . fromStrict . encodeUtf8 . getText . head . head $ rows
  either error return x

get_qa_table_keys :: Integer -> IO [Index]
get_qa_table_keys rid = do
  c <- qa_conn
  ps <- prepareStmt c "SELECT Structure FROM qa_console.TableMeta WHERE RelativeID = ?"
  rows <- query_rows_stmt c ps [toMySQLValue rid]
  close c
  let structure = getText . head . head $ rows
      (cols, indices) = either (error . show) Prelude.id $ parse_create_table structure
  return indices

is_need_fix_unique_key :: Integer -> [Index] -> Index -> Bool
is_need_fix_unique_key rid indices index =
  let idx_name = (name :: Index -> String) index
      r0 = DL.find (\x -> (name :: Index -> String) x == idx_name) indices
  in maybe False (\r -> if r /= index && ((unique :: Index -> Bool) index == False) then True else False) r0

get_qa_has_unique_key_relative_ids :: IO [Integer]
get_qa_has_unique_key_relative_ids = do
  let sql = "select RelativeID from qa_console.TableMeta where RelativeID IN \
            \ (select RelativeID FROM new_console.TableVersionMeta \
            \ where Created >= '2017-09-15 00:00:00' AND Operator = '陈云星(chenyunxing)' AND Version = 1) AND \
            \ (Structure like '%UNIQUE KEY%' OR Structure like '%unique %')"
  c <- new_conn
  rows <- query_rows c sql
  close c
  return $ map (getInt . head) rows

---
--- fix LastColumnId and LastIndexId
---


fix_lastCol_indx_id :: IO ()
fix_lastCol_indx_id = do
   list <- get_last_id_not_right_tables
   handle <- openFile "./fix-last-col-index-id.sql" WriteMode
   mapM_ (do_fix_last_col_idx_id handle) list
   hClose handle

get_last_id_not_right_tables :: IO [(Integer, Integer, Text, Text)]
get_last_id_not_right_tables = do
  c <- new_conn
  ps <- prepareStmt c "SELECT RelativeID, Version, Indices, Columns FROM TableVersionMeta WHERE LastColumnId <= 1 \
                      \ AND Indices != ''"
  rows <- query_rows_stmt c ps []
  close c
  return $ map (\[a, b, c, d] -> (getInt a, getInt b, getText c, getText d)) rows
do_fix_last_col_idx_id :: Handle -> (Integer, Integer, Text, Text) -> IO ()
do_fix_last_col_idx_id handle (rid, version, idxs, cols) = do
  --putStrLn $ show idxs
  --putStrLn $ show cols
  let idxs' = either error Prelude.id $ eitherDecode . fromStrict . encodeUtf8 $ idxs
      cols' = either (\e -> error $ show rid ++ "\n" ++ show e)
                Prelude.id $ eitherDecode . fromStrict . encodeUtf8 $ cols
      max_col_id = DL.foldl max 0 $ map (idNum :: Col -> Int) cols'
      max_idx_id = DL.foldl max 0 $ map (idNum :: Index -> Int) idxs'
  hPutStrLn handle $ "UPDATE TableVersionMeta SET LastColumnId = " ++ (show max_col_id) ++ ", LastIndexId = "
               ++ (show max_idx_id) ++ " WHERE RelativeID = " ++ (show rid) ++ " AND Version = " ++
               (show version) ++ ";"

--
-- generate alter stmt for unique key fixing
--
generate_alter_stmt_for_unique_key :: IO ()
generate_alter_stmt_for_unique_key = do
  content <- readFile "./fix-unique-key.list"
  let cfg_lines = Data.String.lines content
      cfg = map parse_cfg_line cfg_lines
  handle <- openFile "./fix-unique-key-alter.sql" WriteMode
  mapM_ (generate_alter_for_one_unique_key handle) cfg
  hClose handle
  where
    parse_cfg_line :: String -> (Integer, String)
    parse_cfg_line s = let [a, b] = Data.String.words s
                       in (read b, a)

generate_alter_for_one_unique_key :: Handle -> (Integer, String) -> IO ()
generate_alter_for_one_unique_key handle (rid, k) = do
  putStrLn $ (show rid) ++ " > " ++ k
  nc <- new_conn
  ps <- prepareStmt nc "SELECT s.Name, t.Name, v.Indices FROM TableMeta t, SchemaMeta s, TableVersionMeta v \
                 \ WHERE t.SchemaID = s.ID AND t.RelativeID = v.RelativeID AND t.Version = v.Version \
                 \ AND t.RelativeID = ? AND t.Version = 1 AND s.Shard = 1"
  rows <- query_rows_stmt nc ps [toMySQLValue rid]
  close nc
  when (DL.length rows /= 0) $
    do
      let [a, b, c] = head $ rows
          (schema_name, table_name, indices) =
            (getText a, getText b
            , either error Prelude.id $ eitherDecode . fromStrict . encodeUtf8 . getText $ c :: [Index])
          index = maybe (error $ show rid ++ " > " ++ k) Prelude.id $ DL.find (\Index{..} -> name == k) indices
          cols' = ((columns :: Index -> [String]) index)
          cols = DL.intercalate "," $ map (\x -> "`" ++ x ++ "`")cols'
      forM_ [0..511] $ \i -> do
          let t = format "ALTER TABLE {}{}.{} DROP INDEX `{}`, ADD UNIQUE KEY `{}` ({});"
                    (schema_name, (showt (i :: Integer)), table_name, k, k, cols)
          hPutStrLn handle $ unpack $ DTL.toStrict t

---
--- fixup version track
---

fix_qa_table_version_track :: IO ()
fix_qa_table_version_track = do
  c <- new_conn
  ps <- prepareStmt c "SELECT count(1) FROM new_console.TableVersionTrack \
                      \ WHERE TableID = ? AND Version = ?"
  ps0 <- prepareStmt c "SELECT ID FROM new_console.TableMeta WHERE RelativeID = ? AND Env = 'DAILY'"
  ps1 <- prepareStmt c "SELECT ID FROM new_console.TableVersionTrack WHERE TableID = ? AND Version = ?"
  ps2 <- prepareStmt c "INSERT INTO new_console.TableVersionTrack(TableID, Version, SyncID) \
                       \ VALUES(?, ?, ?)"
  tables <- get_table_need_fix c
  mapM_ (fix_version_track ps ps0 ps1 ps2 c) tables
  close c
  where
    get_table_need_fix :: MySQLConn -> IO [(Integer, Integer, Integer)]
    get_table_need_fix c = do
      rows <- query_rows c "SELECT t.RelativeID, t.ID, t.Version FROM new_console.TableMeta t LEFT JOIN \
                   \ new_console.TableVersionTrack vt ON t.ID = vt.TableID AND t.Version = vt.Version \
                   \ WHERE t.version != 0 AND vt.ID is NULL"
      return $ map (\[a,b,c] -> (getInt a, getInt b, getInt c)) rows

fix_version_track :: StmtID -> StmtID -> StmtID -> StmtID -> MySQLConn -> (Integer, Integer, Integer) -> IO ()
fix_version_track ps ps0 ps1 ps2 c (rid, tid, version) = do
  e <- is_track_exist ps c tid version
  when e $ add_track c ps0 ps1 ps2 (rid, tid, version)
  where
    is_track_exist :: StmtID -> MySQLConn -> Integer -> Integer -> IO Bool
    is_track_exist ps c tid v = do
      rows <- query_rows_stmt c ps [toMySQLValue tid, toMySQLValue v]
      return $ DL.length rows /= 0
    add_track :: MySQLConn -> StmtID -> StmtID -> StmtID -> (Integer, Integer, Integer) -> IO ()
    add_track c ps0 ps1 ps2 (rid, tid, version) = do
      putStrLn $ "add track : " ++ show rid ++ " > " ++ show tid ++ " > " ++ show version
      rows <- query_rows_stmt c ps0 [toMySQLValue rid]
      let dev_tid = getInt . head . head $ rows
      rows <- query_rows_stmt c ps1 [toMySQLValue dev_tid, toMySQLValue version]
      let sync_id = getInt . head . head $ rows
      executeStmt c ps2 $ map toMySQLValue [tid, version, sync_id]
      return ()

fix_table_meta_id_incorrect :: IO ()
fix_table_meta_id_incorrect = do
  c <- new_conn
  rows' <- query_rows c "SELECT RelativeID, Version, Columns, Indices, LastColumnId, \
                        \ LastIndexId FROM TableVersionMeta WHERE Version = 1 AND Columns != ''"
  let rows = map (\[id0, v, c, i, lc, li] ->
                    (getInt id0
                    , getInt v
                    , either error Prelude.id $ eitherDecode . fromStrict . encodeUtf8 . getText $ c
                    , either error Prelude.id $ eitherDecode . fromStrict . encodeUtf8 . getText $ i
                    , getInt lc, getInt li))
               rows'
  mapM_ do_fix_id rows
  close c
  return ()
  where
    do_fix_id :: (Integer, Integer, [Col], [Index], Integer, Integer) -> IO ()
    do_fix_id row@(id_, v, c, i, lc, li) = do
      let (ci, ii) = is_need_fix row
          c' = fmap (fix_c c) ci
          i' = fmap (fix_i i) ii
      putStrLn $ (show id_) ++ " > " ++ (show ci) ++ " > " ++ (show ii)
      if isNothing c' && isNothing i' then return ()
        else exec_fix id_ c' i' lc li
      return ()
    exec_fix :: Integer -> Maybe [Col] -> Maybe [Index]  -> Integer -> Integer -> IO ()
    exec_fix rid cols indexs lc li = do
      let mcid = fmap (\x -> DL.foldl max 0 $ map (idNum :: Col -> Int) x) cols
          miid = fmap (\x -> DL.foldl max 0 $ map (idNum :: Index -> Int) x) indexs
      when (isJust mcid && fromJust mcid > fromInteger lc) $ error $ show rid ++ " " ++ show mcid
      when (isJust miid && fromJust miid > fromInteger li) $ error $ show rid ++ " " ++ show miid
      putStrLn $ "fix : -> " ++ (show rid) ++ " > " ++  (show $ isJust cols) ++ " > " ++ (show $ isJust indexs)
      let (sql, params) = if isJust cols && isJust indexs then
            ("UPDATE TableVersionMeta SET Columns = ? ,Indices = ? WHERE RelativeID = ? AND Version = 1"
            , [toMySQLValue . decodeUtf8 . toStrict . encode . fromJust $ cols
              , toMySQLValue . decodeUtf8 . toStrict . encode . fromJust $ indexs
              , toMySQLValue rid])
            else
              if isJust cols then
                ("UPDATE TableVersionMeta SET Columns = ? WHERE RelativeID = ? AND Version = 1"
                , [toMySQLValue . decodeUtf8 . toStrict . encode . fromJust $ cols, toMySQLValue rid])
              else
                ("UPDATE TableVersionMeta SET Indices = ? WHERE RelativeID = ? AND Version = 1"
                , [toMySQLValue . decodeUtf8 . toStrict . encode . fromJust $ indexs, toMySQLValue rid])
      conn <- new_conn
      ps <- prepareStmt conn sql
--      putStrLn $ show sql
--      putStrLn $ show params
      executeStmt conn ps params
      close conn
      return ()

    fix_c cols idx =
      let (a, b) = DL.splitAt idx cols
      in DL.concat [a, map (\col@Col{..} -> col { idNum = idNum + 1} :: Col) b]
    fix_i idxs idx =
      let (a, b) = DL.splitAt idx idxs
      in DL.concat [a, map (\idx@Index{..} -> idx { idNum = idNum + 1} :: Index) b]

    get_col_id :: Col -> Int
    get_col_id Col{..} = idNum
    get_idx_id :: Index -> Int
    get_idx_id Index{..} = idNum
    get_col_name :: Col -> String
    get_col_name Col{..} = name
    get_idx_name :: Index -> String
    get_idx_name Index{..} = name


    is_need_fix :: (Integer, Integer, [Col], [Index], Integer, Integer) -> (Maybe Int, Maybe Int)
    is_need_fix (id_, v, c, i, lc, li) = let
      gb = DL.groupBy (\a b -> (get_col_id a == get_col_id b)) c
      it = fmap (!! 1) $ DL.find (\i -> DL.length i > 1) gb
      gb2 = DL.groupBy (\a b -> (get_idx_id a == get_idx_id b)) i
      it2 = fmap (!! 1) $ DL.find (\i -> DL.length i > 1) gb2
      it_idx = fmap (\x -> fromJust $ DL.elemIndex x c) it
      it2_idx = fmap (\x -> fromJust $ DL.elemIndex x i) it2
      in (it_idx, it2_idx)


main :: IO ()
main = do
  -- get schema only exist in qa, then create them until qa and dev has same schema

  -- create correlation of qa and dev
  -- for each schema, get tables only exist in qa, then create them util qa and dev has same table
  -- setup relativeID for each qa tables
  -- for each schema, set up version and version track of each table in qa where its structure can be found in dev
  -- then, set up version and version track of each table in qa where its strucuture cannot be found in dev
  --       this is done by:
  --       1. parse ddl, then create add record to TableVersionMeta, set its version = (dev-version)
  --       2. update dev version to dev-version + 1
  --       3. add tableVersionTrack for qa
  --       4. update version of last tableVersionTrack for dev
  set_up_schema_correlation
  set_up_table_relative -- async , cannot continue quickly

  --
  --
  modify_qa_with_delta
  set_up_table_version_which_structure_equals -- 626
  cmp_structure_and_setup_qa_tables -- 2020
  set_up_not_equals
  -- dump qa_console and import to new_console
