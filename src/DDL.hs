{-# LANGUAGE RecordWildCards, DeriveGeneric, OverloadedStrings, DuplicateRecordFields #-}
module DDL where

import Data.Text as DT hiding(filter, map, all, head, zipWith)
import Data.Text.Encoding as DE
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Aeson
import GHC.Generics
import Network.HTTP.Simple
import Network.HTTP.Conduit
import Network.HTTP.Types.Status
import Network.HTTP.Types.Header
import Data.Monoid

import Model

instance ToJSON CreateTableInfo
instance ToJSON TableConfig where
  toJSON TableConfig{..} =
    let a = [ "tableName" .= tableName
            , "schemaId" .= schemaId
            , "columns" .= columns
            , "indices" .= indices]
        b = maybe a (\x -> ("shardConfig" .= x):a) shardConfig
    in object b


instance ToJSON DataType_ where
  toJSON DataType_{..} =
    toJSON $ t ++ (maybe "" (\l -> "(" ++ (show l) ++ ")") len) ++ if unsigned then " UNSIGNED" else ""

instance ToJSON Col_ where
  toJSON Col_{..} =
    let i = ["name" .= name, "dataType" .= dataType
            , "autoIncrement" .= autoIncrement, "nullAble" .= null
            , "comment" .= maybe "missing" Prelude.id comment]
        j = maybe i (\x -> ("defaultValue" .= maybe [] (:[]) x):i) defaultValue
        k = maybe j (\x -> ("updateDefaultValue" .= x):j) updateDefaultValue
    in object k

instance ToJSON Index_ where
  toJSON Index_{..} =
    let x = [ "name" .= name , "columns" .= columns , "pk" .= pk, "unique" .= unique ]
        y = maybe x (\k -> ("comment" .= k):x) comment
    in object y

instance ToJSON ShardConfig where
  toJSON ShardConfig{..} =
    let a = [ "column" .= column , "type" .= itype]
        b = maybe a (\x -> ("slice" .= x):a) slice
    in object b

instance ToJSON HdT
instance ToJSON DtC
instance ToJSON DelTableInfo

submit_ddl_operate :: (ToJSON a) => Text -> Text -> a -> IO (Either Text Integer)
submit_ddl_operate cookie url a = do
  init_req <- parseRequest . unpack $ url
  let req = addRequestHeader hCookie (encodeUtf8 cookie)
        . setRequestBodyJSON a $ init_req { method = "POST" }
  resp <- httpLBS req
  let status = getResponseStatus resp
  return $ if status /= status200 then
             Left . decodeUtf8 $ (statusMessage status <> (toStrict . getResponseBody $ resp))
           else Right . read . unpack . decodeUtf8 . toStrict . getResponseBody $ resp
