{-# LANGUAGE RecordWildCards, DeriveGeneric, OverloadedStrings, DuplicateRecordFields #-}
module FixMigration where

import Database.MySQL.Base

import Util hiding(prod_conn);
import Lib;
import Model
import qualified Data.List as DL
import Text.Parsec;
import Data.Text as DT hiding(filter, map, all, head, zipWith)
import Data.Text.Format
import qualified Data.Text.Lazy as DTL
import Data.Text.Encoding as DE
import qualified Data.Text.IO as TIO
import Data.ByteString.Lazy (fromStrict, toStrict)
import TextShow
import Data.Maybe
import Data.Monoid
import Data.String
import qualified Data.HashMap as HM
import Control.Monad
import Control.Exception
import System.IO

online_conn = connect defaultConnectInfo
  { ciHost = "10.9.119.247"
  , ciUser = "shiva"
  , ciPassword = "n4sd9m6W9i29P"
  , ciDatabase = encodeUtf8 "shiva_console" }

-- prod_conn = new_conn
prod_conn = online_conn

all_table_relative :: IO [Integer]
all_table_relative = do
  lists <- bracket online_conn close $ \c -> do
             rows <- query_rows c "SELECT DISTINCT RelativeID FROM TableVersionMeta"
             return $ Prelude.map (getInt . head) rows
  return lists

fix_table_version ::  Integer -> IO ()
fix_table_version rid = do
  lists <- bracket online_conn close $ \c -> do
             ps <- prepareStmt c "SELECT Version, Columns, Indices, LastColumnId, LastIndexId FROM TableVersionMeta WHERE RelativeID = ?"
             rows <- query_rows_stmt c ps [toMySQLValue rid]
             return $ Prelude.map decode_row rows
  let col_mp = DL.foldl setup_col_map HM.empty $ map (\(_, b, _, _, _) -> b) lists
      idx_mp = DL.foldl setup_idx_map HM.empty $ map (\(_, _, c, _, _) -> c) lists
      col_mp' = fix_id col_mp
      idx_mp' = fix_id idx_mp
      lists' = map (convert_version col_mp' idx_mp') lists
  forM_ lists' $ \(v, co, i, lcid, liid) -> do
    bracket online_conn close $ \c -> do
      let col = encode' co
          idx = encode' i
      ps <- prepareStmt c "UPDATE TableVersionMeta SET Columns = ?, Indices = ?, LastColumnId = ?, LastIndexId = ? WHERE RelativeID = ? AND Version = ?"
      executeStmt c ps [toMySQLValue col, toMySQLValue idx, toMySQLValue lcid, toMySQLValue liid, toMySQLValue rid, toMySQLValue v]
  -- putStrLn $ show $ DL.sortBy (\a b -> compare (snd a) (snd b)) $ HM.toList $ col_mp'
  file <- openFile "fix_table.txt" WriteMode
  forM_ lists' $ \(v, c, i, lcid, liid) -> do
                     hPutStrLn file $ show v
                     hPutStrLn file $ unpack $ encode' c
                     hPutStrLn file $ unpack $ encode' i
                     hPutStrLn file $ show lcid
                     hPutStrLn file $ show liid
  return ()
  where
    fix_id mp = let maxid = HM.fold max 0 mp
                    id_map = HM.foldWithKey (\k v r -> if HM.member v r then r else HM.insert v k r) HM.empty mp
                    (mid', mp') = HM.foldWithKey
                        (\k v (mid, r) -> if id_map HM.! v == k then (mid, r) else (mid + 1, HM.insert k (mid + 1) r)) (maxid, mp) mp
                in mp'
    decode_row :: [MySQLValue] -> (Integer, [Col], [Index], Integer, Integer)
    decode_row [a, b, c, d, e] =  (getInt a, cols, indices, getInt d,getInt e)
      where
        cols = either error Prelude.id $ decode' $ getText b
        indices = either error Prelude.id $ decode' $ getText c
    setup_col_map m cols = DL.foldl setup_one_col m cols
    setup_one_col m Col{..} = if HM.member name m then m
      else HM.insert name idNum m
    setup_idx_map m indices = DL.foldl setup_one_idx m indices
    setup_one_idx m Index{..} = if HM.member name m then m
      else HM.insert name idNum m
    convert_version col_mp idx_mp (version, cols, indices, _, _) = (version, cols', indices', lcid', liid')
      where
        col_id Col{..} = idNum
        cols' = DL.sortBy (\a b -> compare (col_id a) (col_id b)) $ map (convert_col col_mp) cols
        idx_id Index{..} = idNum
        indices' = DL.sortBy (\a b -> compare (idx_id a) (idx_id b)) $ map (convert_index idx_mp) indices
        lcid' = HM.fold max 0 col_mp
        liid' = HM.fold max 0 idx_mp
        convert_col m col@Col{..} = col { idNum = m HM.! name} :: Col
        convert_index m index@Index{..} = index { idNum = m HM.! name } :: Index

add_miss_qa rid = do
    qa_structure <- readFile "./qa-version.sql"
    let structure = pack qa_structure
    prepend_table_version prod_conn rid structure
    incr_env_version prod_conn rid "DAILY"
    tid <- get_env_tid prod_conn rid "DAILY"
    prepend_version_track prod_conn tid Nothing
    return ()

prepend_version_meta rid = do
  struct <- readFile "./new-version.sql"
  prepend_table_version prod_conn rid $ pack struct
  incr_env_version prod_conn rid "DAILY"
  incr_env_version prod_conn rid "QA"
  dev_tid <- get_env_tid prod_conn rid "DAILY"
  putStrLn $ "dev tid -> " ++ show dev_tid
  shifts <- prepend_version_track prod_conn dev_tid Nothing
  putStrLn $ "shifts -> " ++ show shifts
  qa_tid <- get_env_tid prod_conn rid "QA"
  putStrLn $ "qa tid -> " ++ show dev_tid
  incr_table_versionTrack_version prod_conn qa_tid
  fixup_version_track_syncId prod_conn qa_tid shifts

fix_table_nullAble_default = do
  all_tbs <- all_tables
  let need_fix_tbs = DL.filter need_fix all_tbs
  --forM_ need_fix_tbs do_fix_column_meta
  putStrLn $ show $ fmap (\(a, b, c) -> (a,b)) $ need_fix_tbs
  return ()
  where
    need_fix :: (Integer, Integer, [Col]) -> Bool
    need_fix  (rid, version, cols) = DL.any col_need_fix cols
    col_need_fix :: Col -> Bool
    col_need_fix Col{..} = not null && defaultValue == Just Nothing
    all_tables = do
      bracket online_conn close $ \c -> do
        rows <- query_rows c "SELECT RelativeID, Version, Columns FROM TableVersionMeta WHERE Columns is NOT NULL AND Columns != ''"
        return $ DL.map convert rows
    convert [a, b, c] = (getInt a, getInt b
                        , eitherErrorS . decode' . getText $ c)
    do_fix_column_meta (rid, version, cols) = do
      let cols' = DL.map fix_col cols
      bracket online_conn close $ \c -> do
        ps <- prepareStmt c "UPDATE TableVersionMeta SET Columns = ? WHERE RelativeID = ? AND Version = ? "
        executeStmt c ps [toMySQLValue . encode' $ cols', toMySQLValue rid, toMySQLValue version]
      where fix_col c@Col{..} = c { defaultValue = Nothing } :: Col

fix_nullAble = do
  all_tbs <- all_tables
  let need_fix_tbs = DL.filter need_fix all_tbs
  putStrLn $ show $ fmap (\(a,b,c,d) -> (a,b)) $ need_fix_tbs
  --forM_ need_fix_tbs test_prod_reference
  -- forM_ need_fix_tbs do_fix_nullAble
  where
    test_prod_reference (rid, version, _, _) = do
      bracket online_conn close $ \c -> do
        ps <- prepareStmt c "SELECT ID FROM TableMeta WHERE RelativeID = ? AND Version = ? AND Env = 'PROD'"
        rows <- query_rows_stmt c ps [toMySQLValue rid, toMySQLValue version]
        if DL.length rows /= 0 then
          do
            let tid = getInt . DL.head . DL.head $ rows
            putStrLn $ show (rid, version, tid)
          else return ()
    do_fix_nullAble (rid, version, cols, colsOK) = do
      putStrLn $ show (rid, version)
      bracket online_conn close $ \c -> do
        ps <- prepareStmt c "UPDATE TableVersionMeta SET Columns = ? WHERE RelativeID = ? AND Version = ?"
        executeStmt c ps [toMySQLValue . encode' $ colsOK, toMySQLValue rid, toMySQLValue version]
    all_tables = do
      bracket online_conn close $ \c -> do
        rows <- query_rows c "SELECT RelativeID, Version, Columns, DDL FROM TableVersionMeta WHERE Columns is NOT NULL AND Columns != ''"
        return $ DL.map convert rows
    convert [a, b, c, d] = (getInt a, getInt b
                           ,eitherErrorS . decode' . getText $ c
                           , nCols)
      where
        dt = getText d
        (nCols, _) = either (\e -> error $ (DT.unpack dt) ++ "\n" ++ show e) Prelude.id $ parse_create_table dt
    need_fix :: (Integer, Integer, [Col], [Col]) -> Bool
    need_fix (a, b, cols', cols) = DL.any (col_need_fix cols') cols
    col_need_fix cols c =
      let c' = fromJust $ DL.find (\cc -> ((Model.name :: (Col -> String)) cc) == ((Model.name :: (Col -> String))  c)) cols
      in ((Model.null :: Col -> Bool) c') /= ((Model.null :: Col -> Bool) c)


fix_table_columns_and_indices rid version = do
  (cols, indices, struct) <- get_table_version_info prod_conn rid version
  let (cols', indices') = eitherErrorS $ parse_create_table struct
      cols'' = DL.map (set_up_col_id cols) cols'
      indices'' = DL.map (set_up_idx_id indices) indices'
  update_column_indices prod_conn rid version (Util.encode' cols'') (Util.encode' indices'')
  return ()
  where
    col_name :: Col -> String
    col_name Col{..} = name
    set_up_col_id cols col =
      let c' = fromMaybe (error "cannot found") $ DL.find (\c -> col_name c == col_name col) cols
          cid = ((idNum :: Col -> Int) c')
      in  col { idNum = cid } :: Col
    set_up_idx_id indices idx =
      let i' = fromMaybe (error "cannot found") $ DL.find (\i -> idx_name i == idx_name i) indices
          iid = ((idNum :: Index -> Int) i')
      in  idx { idNum = iid } :: Index
    update_column_indices conn rid version cols indices = do
      bracket conn close $ \c -> do
        ps <- prepareStmt c "UPDATE TableVersionMeta SET Columns = ?, Indices = ? WHERE RelativeID = ? AND Version = ?"
        executeStmt c ps [toMySQLValue cols, toMySQLValue indices, toMySQLValue rid, toMySQLValue version]


get_table_version_info :: IO MySQLConn -> Integer -> Integer -> IO ([Col], [Index], Text)
get_table_version_info conn rid version = do
  bracket conn close $ \c -> do
    ps <- prepareStmt c "SELECT Columns, Indices, DDL \
                       \ FROM TableVersionMeta WHERE RelativeID = ? AND Version = ?"
    row <- query_single_row_stmt c ps $ fmap toMySQLValue [rid, version]
    let [a, b, c] = row
        (cols, indices, struct) = (getText a, getText b, getText c)
        cols' = eitherError $ Util.decode' cols
        indices' = eitherError $ Util.decode' indices
    return (cols', indices', struct)
