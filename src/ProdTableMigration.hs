{-# LANGUAGE OverloadedStrings, RecordWildCards, DuplicateRecordFields #-}
module ProdTableMigration where


import ProdMigrationCommon
import Util
import Model
import DDL
import Data.Text
import qualified Data.Text.IO as TIO
import Data.Text.Encoding
import Data.Text.Format
import qualified Data.Text.Lazy as DTL
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as DL
import Control.Exception
import Control.Monad
import qualified Data.List.Split as S
import Control.Applicative
import Data.Monoid
import Data.Maybe
import TextShow
import Database.MySQL.Base

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------ set up prod <-> qa morphism -------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

-- set prod <-> qa table relative by tableName
-- create table test.t0 (RelativeID Int, ProdID Int);
-- insert into test.t0 select a.RelativeID as RelativeID, c.ID as ProdID from  new_console.TableMeta a , new_console.SchemaCorrelation b, prod_console.TableMeta c where a.Name = c.Name AND a.SchemaID = b.QaID AND c.SchemaID = b.ProdID;
-- update prod_console.TableMeta a SET RelativeID = (select RelativeID from test.t0 where ProdID = a.ID);

setup_table_relative = do
  bracket new_conn close $ \c -> do
    execute_ c "DROP TABLE IF EXISTS test.t0"
    execute_ c "create table test.t0 (RelativeID Int, ProdID Int)"
    execute_ c "insert into test.t0 select a.RelativeID as RelativeID, c.ID as ProdID from \
              \ new_console.TableMeta a , new_console.SchemaCorrelation b, \
              \ prod_console.TableMeta c where a.Name = c.Name AND \
              \ a.SchemaID = b.QaID AND c.SchemaID = b.ProdID;"
    execute_ c "update prod_console.TableMeta a SET RelativeID = \
               \ (select RelativeID from test.t0 where ProdID = a.ID);"

-- get table prod only
table_prod_qa_miss = do
  bracket prod_conn close $ \c -> do
    rows <- query_rows c "select SchemaID,ID from prod_console.TableMeta WHERE RelativeID = 0"
    return $ DL.map (\[a, b] -> (getInt a, getInt b)) rows

table_prod_only = do
  a <- table_prod_qa_miss
  b <- table_prod_dev_exists
  return $ a DL.\\ b

table_prod_dev_exists = do
  bracket prod_conn close $ \c -> do
    rows <- query_rows c "select a.SchemaID,a.ID from prod_console.TableMeta a, \
                         \ new_console.TableMeta b, new_console.SchemaCorrelation \
                         \ c WHERE a.SchemaID = c.ProdID AND a.Name = b.Name AND \
                         \ b.SchemaID = c.DailyID AND a.RelativeID = 0;"
    return $ DL.map (\[a, b] -> (getInt a, getInt b)) rows
-- create table prod only
-- 1. create in dev
-- 2. sync from dev to qa
--
-- after this fix, should run setup_tableRelative again
--    to wire up prod <-> qa table relation
create_table_prod_only tables = do
  cfgs <- forM tables get_table_create_info
  let cfgss = S.chunksOf 100 cfgs
  forM_ cfgss do_create_tables
  where
    do_create_tables cfgs = do
      fmap (either (error . unpack) Prelude.id) $ submit_create_table "rds-auto create table dev missing " cfgs
    submit_create_table title tableConfig = do
      ret <- submit_ddl_operate cookie
               "http://pre-rds.qima-inc.com/api/ddl/operate/daily/plan/create_table"
               CreateTableInfo{..}
      putStrLn $ show ret
      return ret

sync_table_prod_only tables = do
  cfgs <- forM tables get_table_sync_info   -- (dev_tableId, dev_version_track_id, ddl)
  let cfgss = S.chunksOf 100 cfgs
  forM_ cfgss do_sync_tables
  where
    get_table_sync_info (sid, tid) = do
      bracket new_conn close $ \c -> do
        ps <- prepareStmt c "SELECT nt.ID FROM new_console.SchemaCorrelation c, \
                            \ prod_console.TableMeta t, new_console.TableMeta nt \
                            \ WHERE c.DailyID = nt.SchemaID AND nt.Name = t.Name \
                            \ AND c.ProdID = t.SchemaID AND t.ID = ?"
        rows <- query_rows_stmt c ps [toMySQLValue tid]
        let dev_tid = getInt . DL.head . DL.head $ rows
        ps <- prepareStmt c "SELECT ID FROM new_console.TableVersionTrack WHERE Version = 1 AND TableID = ?"
        rows <- query_rows_stmt c ps [toMySQLValue dev_tid]
        let sync_id = getInt . DL.head . DL.head $ rows
        ps <- prepareStmt c "SELECT a.DDL FROM new_console.TableVersionMeta a left JOIN \
                            \ new_console.TableMeta b on a.RelativeID = b.RelativeID \
                            \ WHERE a.Version = 1 AND b.ID = ?"
        rows <- query_rows_stmt c ps [toMySQLValue dev_tid]
        let ddl = getText . DL.head . DL.head $ rows
            version = 1
            versionId = fromInteger sync_id
            tableId = fromInteger dev_tid
        return SyncTableConfig{..}
    do_sync_tables :: [SyncTableConfig] -> IO Integer
    do_sync_tables cfgs = do
      fmap (either (error . unpack) Prelude.id) $ submit_sync_table "rds-auto sync table dev -> qa" cfgs
    submit_sync_table title tableConfig = do
      r <- submit_ddl_operate cookie
             "http://pre-rds.qima-inc.com/api/ddl/operate/qa/plan/sync_table"
             SyncTableInfo{..}
      putStrLn $ show r
      return r

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------ set up versionMeta and VersionTrack -----------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

-- setup if structure equals found
-- 1. add prod's versionTrack
-- 2. setup prod's version to matched
setup_meta_by_structure_cmp = do
  all_tables <- get_all_tables -- (RelativeID, TableID, Structure)
  forM_ all_tables $ cmp_and_setup_meta_if_equals
  where
    cmp_and_setup_meta_if_equals (rid, tid, struct) = do
      putStrLn $ " rid = " ++ (show rid) ++ ", tid = " ++ (show tid)
      let (cols, indices) = eitherErrorS $ parse_create_table struct
      table_versions <- get_table_versions new_conn rid
      let match = DL.find (struct_equals cols indices) table_versions
      case match of
        Nothing -> return ()
        Just m -> setup_meta rid tid m
    setup_meta rid tid (version, _, _, _, _) = do
      bracket prod_conn close $ \c -> do
        ps <- prepareStmt c "UPDATE TableMeta SET Version = ? WHERE ID = ?"
        executeStmt c ps [toMySQLValue version, toMySQLValue tid]
        return ()
      qa_tid <- get_env_tid new_conn rid "QA"
      putStrLn $ " qa id  = " ++ show qa_tid
      sync_id <- bracket new_conn close $ \c -> do
        ps <- prepareStmt c "SELECT ID FROM TableVersionTrack WHERE TableID = ? AND Version = ?"
        rows <- query_rows_stmt c ps [toMySQLValue qa_tid, toMySQLValue version]
        return $ if (DL.length rows) == 0 then Nothing else Just . getInt . DL.head . DL.head $ rows
      putStrLn $ " sync_id  = " ++ show sync_id
      sid <- maybe (append_version_track new_conn rid qa_tid version) return sync_id
      add_table_version_track new_conn tid sid version
      return ()
    append_version_track conn rid qa_tid version = do
      dev_tid <- get_env_tid conn rid "DAILY"
      dev_sid <- get_version_track_id conn dev_tid version
      add_table_version_track conn qa_tid dev_sid version
      qa_version <- get_table_current_version conn qa_tid
      qa_rollback_sid <- get_version_track_id conn qa_tid qa_version
      add_table_version_track conn qa_tid qa_rollback_sid qa_version
      qa_vt_id <- get_version_track_id conn qa_tid version
      return qa_vt_id
    get_version_track_id conn tid version = do
      bracket conn close $ \c -> do
        ps <- prepareStmt c "SELECT ID FROM TableVersionTrack WHERE TableID = ? AND Version = ?"
        did <- query_single_stmt c ps [toMySQLValue tid, toMySQLValue version]
        return $ getInt did
    add_table_version_track conn tid sid version = do
      bracket conn close $ \c -> do
        ps <- prepareStmt c "INSERT INTO TableVersionTrack(TableID, Version, SyncID, Operator) VALUES(?,?,?,?)"
        executeStmt c ps [ toMySQLValue tid
                     , toMySQLValue version
                     , toMySQLValue sid
                     , toMySQLValue ("陈云星(chenyunxing)" :: Text)]
    get_table_current_version conn tid = do
      bracket conn close $ \c -> do
        ps <- prepareStmt c "SELECT Version FROM TableMeta WHERE ID = ?"
        v <- query_single_stmt c ps [toMySQLValue tid]
        return $ getInt v
    struct_equals cols indices (_, cols', indices', _, _) =
      let al = DL.length cols
          bl = DL.length indices
          al' = DL.length cols'
          bl' = DL.length indices'
      in al == al' && bl == bl' && cols_equals cols cols' && idxes_equals indices indices'
      where
        cols_equals c0 c1 =
          let c0' = DL.sortBy (\a b -> compare (col_name a) (col_name b)) c0
              c1' = DL.sortBy (\a b -> compare (col_name a) (col_name b)) c1
              c0c1' = DL.zip c0' c1'
          in DL.all (\(x ,y) -> (x { idNum = 0 } :: Col) == (y { idNum = 0 } :: Col)) c0c1'
        idxes_equals i0 i1 =
          let i0' = DL.sortBy (\a b -> compare (idx_name a) (idx_name b)) i0
              i1' = DL.sortBy (\a b -> compare (idx_name a) (idx_name b)) i1
              i0i1' = DL.zip i0' i1'
          in DL.all (\(x ,y) -> (x { idNum = 0 } :: Index) == (y { idNum = 0 } :: Index)) i0i1'


get_all_tables = do
  bracket prod_conn close $ \c -> do
    rows <- query_rows c "SELECT RelativeID, ID, Structure FROM TableMeta"
    return $ DL.map (\[a, b, c] -> (getInt a, getInt b, getText c)) rows

-- setup structure not equals to any Version History
-- 1. create new version, and prepend : as the first version
-- 2. modify each env's versionTrack
setup_meta_by_prepend = do
  tables <- get_table_un_wired
  -- forM_ (DL.take 1 tables) $ setup_meta
  forM_ tables $ setup_meta
  where
    setup_meta (rid, tid, struct) = do
      putStrLn $ "rid = " ++ (show rid) ++ ", tid = " ++ (show tid)
      -- set version = 1
      incr_env_version prod_conn rid "PROD"
      -- add this structure to the first version of the Table
      prepend_table_version new_conn rid struct
      -- increase version to version + 1 IN `DAILY` AND `QA`
      incr_env_version new_conn rid "DAILY"
      incr_env_version new_conn rid "QA"
      -- daily table id
      dev_tid <- get_env_tid new_conn rid "DAILY"
      -- prepend version_track of daily table
      dev_shifts <- prepend_version_track new_conn dev_tid Nothing -- returned the shifts [(a, b)] means a -> b
      putStrLn $ "dev shifts : " ++ show dev_shifts
      -- qa table id
      qa_tid <- get_env_tid new_conn rid "QA"
      qa_shifts <- prepend_version_track new_conn qa_tid $ Just dev_shifts
      putStrLn $ "qa shifts : " ++ show qa_shifts
      -- add versionTrack for prod env's table, which synced from the qa
      add_prod_version_track new_conn tid (fst . DL.head $ qa_shifts)

get_table_un_wired :: IO [(Integer, Integer, Text)] -- relativeID, ID, Structure
get_table_un_wired = do
  bracket prod_conn close $ \c -> do
    rows <- query_rows c "SELECT RelativeID, ID, Structure FROM TableMeta WHERE Version = 0"
    return $ DL.map (\[a, b, c] -> (getInt a, getInt b, getText c)) rows

add_prod_version_track :: IO MySQLConn -> Integer -> Integer -> IO OK
add_prod_version_track conn tid syncId = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "INSERT INTO TableVersionTrack(TableID, Version, SyncID, Operator) VALUES(?,?,?,?)"
    executeStmt c ps [ toMySQLValue tid
                     , toMySQLValue (1 :: Integer)
                     , toMySQLValue syncId
                     , toMySQLValue ("陈云星(chenyunxing)" :: Text)]
