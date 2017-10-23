{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}

module Lib ( a_createtable, a_tables, a_shardconfig) where

import Model
import Text.Parsec.Prim
import Text.Parsec.Combinator
import Text.Parsec.Char
import Data.Maybe
import qualified Data.Text as DT
import Data.Char as C
import Data.Either
import GHC.Generics
import qualified Data.List as DL
import qualified Control.Applicative as CA (Alternative(empty), (<|>))
import Control.Monad

qt :: (Stream s m Char) => Char -> Char -> ParsecT s u m a -> ParsecT s u m [a]
qt l r p = char l *> manyTill p (char r)

str_qt :: Stream s m Char => ParsecT s u m a -> ParsecT s u m [a]
str_qt p = char '"' *> manyTill p (char '"') <|> char '\'' *> manyTill p (char '\'')

choice' :: Stream s m  Char => [ParsecT s u m a] -> ParsecT s u m a
choice' = foldl (\r i -> r <|> try i) CA.empty

qt_optional :: Stream s m Char => Char -> Char -> ParsecT s u m a -> ParsecT s u m [a]
qt_optional l r p = (qt l r p) <|> manyTill p (lookAhead $ oneOf "\n,) ")

str_qt_optional :: Stream s m Char => ParsecT s u m a -> ParsecT s u m [a]
str_qt_optional p = str_qt p <|> manyTill p (lookAhead $ oneOf "\n,) ")

symbol :: (Stream s m Char) => String -> ParsecT s u m String
symbol a = string a

lowerStr :: String -> String
lowerStr = map toLower

a_datatype_optional_length :: (Stream s m Char) => String -> ParsecT s u m String
a_datatype_optional_length ts = (string ts <|> string (lowerStr ts)) <* optional (qt '(' ')' digit)

a_inttype :: (Stream s m Char) => String -> ParsecT s u m DataType
a_inttype ts = do
   t <- a_datatype_optional_length ts
   u <- optionMaybe $ try (spaces *> (string "UNSIGNED" <|> string "unsigned"))
   return $ DataType { t = ts, len = Nothing, unsigned = (isJust u)}

a_decimal :: (Stream s m Char) => ParsecT s u m DataType
a_decimal = do
  string "DECIMAL" <|> string "decimal"
  optional $ try (qt '(' ')' (char ',' <|> digit))
  u <- optionMaybe $ try (spaces *> (string "UNSIGNED" <|> string "unsigned"))
  return $ DataType { t = "DECIMAL", len = Nothing, unsigned = (isJust u)}

a_chartype :: (Stream s m Char) => String -> ParsecT s u m DataType
a_chartype ts = do
  t <- string ts <|> string (lowerStr ts)
  len <- qt '(' ')' digit
  return $ DataType { t = ts, len = (Just $ read len), unsigned = False }

a_texttype :: (Stream s m Char) => String -> ParsecT s u m DataType
a_texttype ts = do
  t <- string ts <|> string (lowerStr ts)
  return $ DataType { t = ts, len = Nothing, unsigned = False }

a_blobtype :: (Stream s m Char) => String -> ParsecT s u m DataType
a_blobtype ts = do
  t <- string ts <|> string (lowerStr ts)
  return $ DataType { t = ts, len = Nothing, unsigned = False }

a_enumtype :: (Stream s m Char) => ParsecT s u m DataType
a_enumtype = do
  string "enum" <|> string "ENUM"
  between (char '(') (char ')')
    (((optional spaces) *> (qt '\'' '\'' anyChar) <* (optional spaces)) `sepBy` (symbol ","))
  return $ DataType { t = "VARCHAR", len = Just 256, unsigned = False }

a_datetimetype :: (Stream s m Char) => ParsecT s u m DataType
a_datetimetype = do
  string "DATETIME" <|> string "datetime"
  optional $ try (qt '(' ')' digit)
  return $ DataType { t = "DATETIME", len = Nothing, unsigned = False }

a_yeartype :: (Stream s m Char) => ParsecT s u m DataType
a_yeartype = do
  string "year" <|> string "YEAR"
  optional $ try (qt '(' ')' digit)
  return $ DataType { t = "YEAR", len = Nothing, unsigned = False }

a_floattype :: (Stream s m Char) => String -> ParsecT s u m DataType
a_floattype ts = do
  t <- string ts <|> string (lowerStr ts)
  optional $ try (spaces *> char '(' *> (many digit) *> char ',' *> (many digit) *> char ')')
  u <- optionMaybe $ try (spaces *> (string "UNSIGNED" <|> string "unsigned"))
  return $ DataType { t = ts, len = Nothing, unsigned = isJust u }

a_datetype :: (Stream s m Char) => ParsecT s u m DataType
a_datetype = do
  string "DATE" <|> string "date"
  return $ DataType { t = "DATE", len = Nothing, unsigned = False }

a_timestamptype :: (Stream s m Char) => ParsecT s u m DataType
a_timestamptype = do
  string "TIMESTAMP" <|> string "timestamp"
  return $ DataType { t = "TIMESTAMP", len = Nothing, unsigned = False }

a_timetype :: (Stream s m Char) => ParsecT s u m DataType
a_timetype = do
  string "TIME" <|> string "time"
  return $ DataType { t = "TIME", len = Nothing, unsigned = False }

a_dataType :: (Stream s m Char) => ParsecT s u m DataType
a_dataType = choice' [ a_inttype "INT"
                    , a_inttype "BIGINT"
                    , a_inttype "SMALLINT"
                    , a_inttype "TINYINT"
                    , a_inttype "MEDIUMINT"
                    , a_inttype "BIT"
                    , a_decimal
                    , a_floattype "FLOAT"
                    , a_floattype "DOUBLE"
                    , a_chartype "CHAR"
                    , a_chartype "VARCHAR"
                    , a_chartype "BINARY"
                    , a_chartype "VARBINARY"
                    , a_texttype "TEXT"
                    , a_texttype "TINYTEXT"
                    , a_texttype "MEDIUMTEXT"
                    , a_texttype "LONGTEXT"
                    , a_blobtype "BLOB"
                    , a_blobtype "MEDIUMBLOB"
                    , a_blobtype "LONGBLOB"
                    , a_enumtype
                    , a_datetimetype
                    , a_datetype
                    , a_timestamptype
                    , a_timetype
                    , a_yeartype]

a_columnname :: (Stream s m Char) => ParsecT s u m String
a_columnname =  notFollowedBy (pk_prefix <|> pindex_prefix)
  *> (qt_optional '`' '`' (alphaNum <|> char '_' <|> char ' '))

a_nullable :: (Stream s m Char) => ParsecT s u m Bool
a_nullable = do
  s <- spaces *> ((try $ string "NOT NULL") <|> (try $ string "NULL"))
  return (s /= "NOT NULL")

a_defaultvalue :: (Stream s m Char) => ParsecT s u m (Maybe String)
a_defaultvalue =  do
  space *> (string "default" <|> string "DEFAULT") *> space
  ((try $ string "NULL") *> return Nothing) <|> (str_qt_optional anyChar >>= return . Just)

a_updatedefaultvalue :: (Stream s m Char) => ParsecT s u m String
a_updatedefaultvalue = space *> (string "ON UPDATE" <|> string "on update") *> space *> str_qt_optional (anyChar)

a_comment :: (Stream s m Char) => ParsecT s u m String
a_comment = space *> (string "COMMENT" <|> string "comment") *> space *> str_qt anyChar

a_autoincrement :: (Stream s m Char) => ParsecT s u m Bool
a_autoincrement = space *> (string "AUTO_INCREMENT" <|> string "auto_increment") *> return True

a_pk :: (Stream s m Char) => ParsecT s u m Bool
a_pk = space *> (string "PRIMARY" <|> string "primary") *> (string " KEY" <|> string " key") *> return True

a_collate :: (Stream s m Char) => ParsecT s u m String
a_collate = do
  space
  spaces
  string "collate" <|> string "COLLATE"
  space
  spaces
  many (alphaNum <|> char '_')

a_charset :: (Stream s m Char) => ParsecT s u m String
a_charset = do
  space
  spaces
  string "CHARACTER SET" <|> string "character set"
  space
  spaces
  many (alphaNum <|> char '_')

a_column :: (Stream s m Char) => ParsecT s u m Col
a_column = do
  name <- a_columnname
  spaces
  dt <- a_dataType
  optional $ try a_charset
  optional $ try a_collate
  optional $ try (spaces *> string "zerofill")
  nullable1 <- optionMaybe  $ try a_nullable
  defaultValue <-  optionMaybe $ try a_defaultvalue
  updateDefault <- optionMaybe $ try a_updatedefaultvalue
  pk <- option False $ try a_pk
  autoIncrement <- option False $ try a_autoincrement
  nullable2 <- optionMaybe $ try a_nullable
  comment <- optionMaybe $ try a_comment
  let nullable = maybe True Prelude.id $ nullable1 CA.<|> nullable2
  return $ Col { idNum = 2
               , name = name
               , dataType = dt
               , pk = pk
               , autoIncrement = autoIncrement
               , null = nullable
               , defaultValue = defaultValue
               , updateDefaultValue = updateDefault
               , comment = comment
               }

data KeyType = PK | UNIQUE_KEY | KEY deriving(Eq, Show)

pk_prefix :: Stream s m Char => ParsecT s u m KeyType
pk_prefix = (string "primary key" <|> string "PRIMARY KEY") *> pure PK

a_pkindex :: Stream s m Char => ParsecT s u m (KeyType, String)
a_pkindex = pk_prefix *> pure (PK, "PK")

pindex_prefix :: Stream s m Char => ParsecT s u m KeyType
pindex_prefix = do
  u <- optionMaybe $ string "unique" <|> string "UNIQUE" <|> string "FULLTEXT" <|> string "fulltext"
  spaces
  string "key" <|> string "index" <|> string "KEY" <|> string "INDEX"
  return (if (isJust u) then UNIQUE_KEY else KEY)

a_plainindex :: Stream s m Char => ParsecT s u m (KeyType, String)
a_plainindex = do
  kt <- pindex_prefix
  name <- space *> qt '`' '`' (char ' ' <|> alphaNum <|> char '_' <|> char '+' <|> char '-' <|> char ',' <|> char '\b')
  return $ (kt, name)
a_constraint :: Stream s m Char => ParsecT s u m ()
a_constraint = do
  string "CONSTRAINT" <|> string "constraint"
  spaces
  manyTill anyChar (lookAhead $ char ',' <|> char '\n')
  return ()

a_index :: Stream s m Char => ParsecT s u m Index
a_index = do
  (kt, name) <- a_pkindex <|> a_plainindex
  spaces
  columns <- between (char '(') (char ')')
     (((qt '`' '`' (alphaNum <|> char '_')) <* (optional (qt '(' ')' alphaNum))) `sepBy` symbol ",")
  optionMaybe $ try (spaces *> string "USING " *> (string "BTREE" <|> string "HASH"))
  comment <- optionMaybe $
    try (space *> (string "COMMENT" <|> string "comment") *> space *> spaces *> str_qt anyChar)
  return $ Index { idNum = 1
                 , name = name
                 , columns = columns
                 , unique = (kt == UNIQUE_KEY)
                 , pk = kt == PK
                 , comment = comment
                 }

a_createtable :: Stream s m Char => ParsecT s u m CreateTable
a_createtable = do
  string "CREATE TABLE" <|> string "create table"
  space
  spaces
  tablename <- qt_optional '`' '`' (alphaNum <|> char '_' <|> char '.' <|> char '-')
  spaces
  (indices, cols) <- between (char '(') (char ')') $
         nl_space_around *> column_index_defs <* nl_space_around
  manyTill anyChar (eof <|> lookAhead (char ';' >> return ()))
  return $ CreateTable { tableName = tablename
                       , columns = setColIdNum cols
                       , indices = setIndexIdNum $ filter (\Index{..} -> name /= "__X__") indices }
  where
    setColIdNum = map (\(a, b) -> b { idNum = a } :: Col) . zip [1..]
    setIndexIdNum = map (\(a, b) -> b { idNum = a } :: Index) . zip [1..]
    skipIndex = Left (Index { idNum = 0, name = "__X__", columns = [], unique = False, pk = False, comment = Nothing })
    column_index_defs = (column_or_index `sepBy` seperator) >>= return . partitionEithers
    column_or_index = (try a_constraint *> return skipIndex) <|> (try a_index >>= return . Left) <|> (a_column >>= return . Right)
    seperator = try (spaces *> (symbol ",") *> spaces *> (optional $ symbol "\n") *> spaces)
    nl_space_around = spaces *> (optional $ symbol "\n") *> spaces

a_tables :: Stream s m Char => ParsecT s u m [CreateTable]
a_tables = a_createtable `endBy` (eof <|> (char ';' *> optional (char '\n')))

a_shard_type :: Stream s m Char => ParsecT s u m String
a_shard_type = do
  x <- choice [ string "no_string"
              , string "long"
              , string "string"]
  return $ map toUpper x
a_shard_slice :: Stream s m Char => ParsecT s u m (Maybe (Integer, Integer))
a_shard_slice = do
  (char ':' *> eof >> return Nothing) <|> (a_slice >>= return . Just)
  where
    a_slice = do
      l <- many1 digit
      char ':'
      r <- many1 digit
      return (read l, read r)

a_shardconfig :: Stream s m Char => ParsecT s u m ShardConfig
a_shardconfig = do
  string "flat_" *> many digit *> (char '_')
  column <- fmap DT.pack $ manyTill (alphaNum <|> char '_') (lookAhead . try $ char '_' *> a_shard_type)
  char '_'
  itype <- fmap DT.pack a_shard_type
  slice' <- optionMaybe $ char '_' *> a_shard_slice
  let slice = fmap (\(x, y) -> [x, y]) $ maybe Nothing Prelude.id slice'
  return ShardConfig{..}
