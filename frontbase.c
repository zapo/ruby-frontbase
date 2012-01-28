/***************************************************
 * Ruby driver for FrontBase
 * 
 * author: Cail Borrell
 * modified by Mike Laster for ActiveRecord support
 *
 * version: 1.0.0 
 ***************************************************/

#define RUBY_BINDINGS_VERSION "1.0.1"

#include "ruby.h"

#if defined(__APPLE__)
#include "/Library/FrontBase/include/FBCAccess/FBCAccess.h"
#else
#warning I don't know where FBCAccess.h is installed on non-OSX platforms
#include "/usr/local/FrontBase/include/FBCAccess.h"
#endif

#pragma mark --- structure definitions ---

/*typedef struct FBCLob
{
   unsigned char  kind;               // 0 => direct, 1 => indirect
   char           handleAsString[28]; // @'<24 hex digits>'\0
} FBCLob;

typedef union FBCColumn FBCColumn;

union FBCColumn
{
   char               tinyInteger;
   short              shortInteger;
   int                integer;
   int                primaryKey;
   long long          longInteger;
   unsigned char      boolean;
   char               character[0x7fffffff];
   double             numeric;
   double             real;
   double             decimal;
   FBCBitValue        bit;
   char               date[11];   // YYYY-MM-DD
   int                unformattedDate;
   char               time[9];    // HH:MM:SS
   char               timeTZ[34];   // YYYY-MM-DD HH:MM:SS.sssss+HH:MM
   char               timestampTZ[34];
   char               timestamp[28];
   char               yearMonth[64];    
   char               dayTime[32];  //  days:hh:ss.ffffff
   FBCLob             blob;
   FBCLob             clob;
   double             rawDate;
   FBCUnformattedTime rawTime;
   FBCUnformattedTime rawTimeTZ;
   FBCUnformattedTime rawTimestamp;
   FBCUnformattedTime rawTimestampTZ;
   int                rawYearMonth;
   double             rawDayTime;
};

typedef FBCColumn* FBCRow;*/

struct fbsqlconnect
{
   int port;
   char *host; 
   char *database;
   char *user;
   char *password;
   char *databasePassword;
   
   FBCExecHandler *fbeh;
   FBCDatabaseConnection *fbdc;
   FBCMetaData *meta;
};

struct fbsqlresult
{
   int rows;
   int cols;

   FBCRow *row;
   void *rawData;
   
   FBCDatabaseConnection *fbdc;
   FBCMetaData *md, *meta;
   FBCRowHandler* rowHandler;
   char* fetchHandle;
   int resultCount;
   int currentResult;
   int rowIndex;

   int currentRow;
};

struct fbsqllob
{
   FBCDatabaseConnection *fbdc;
   FBCBlobHandle *handle;
   char* bhandle;
   int size;
   int type;
};

typedef struct fbsqlconnect FBSQL_Connect;
typedef struct fbsqlresult  FBSQL_Result;
typedef struct fbsqllob     FBSQL_LOB;

#pragma mark --- Ruby class definitions ---

static VALUE rb_cFBConn;    // FBSQL_Connect Class
static VALUE rb_cFBResult;  // FBSQL_Result Class
static VALUE rb_cFBLOB;     // FBSQL_LOB Class
static VALUE rb_cFBError;   // FBError Class (Exception)

static VALUE fbconn_query _((VALUE, VALUE));

static int  fetch_fbresult _((FBSQL_Result*, int, int, VALUE*));
static int  fetch_next_row _((FBSQL_Result*, VALUE*));
static void fetch_convert_value _((FBSQL_Result*, int, VALUE*));

static VALUE fbresult_result _((VALUE));
static VALUE fbresult_clear _((VALUE));
static VALUE fbresult_query _((VALUE));

// Garbage Colleciton Helper prototypes
static void free_fbconn _((FBSQL_Connect*));
static void free_fbresult _((FBSQL_Result*));
static void free_fblob _((FBSQL_LOB*));

// helper function prototypes
static FBSQL_Connect * get_fbconn _((VALUE));
static FBSQL_Result * get_fbresult _((VALUE));
static FBSQL_LOB * get_fblob _((VALUE));
static VALUE checkMetaData _((FBCDatabaseConnection*, FBCMetaData*));

#define FRONTBASE_COMMAND_OK 1
#define FRONTBASE_ROWS_OK    2
#define FRONTBASE_UNIQUE_OK  3

#define FB_ERR_NO_CONNECTION 1

#define FETCH_SIZE 4096

#pragma mark --- garbage collector helper functions ---

//
// free_fbconn()
//

static void free_fbconn(ptr) FBSQL_Connect *ptr;
{
   fbcdcClose(ptr->fbdc);
   free(ptr); // !!! Change to use Ruby memory functions
}

//
// free_fblob()
//

static void free_fblob(ptr) FBSQL_LOB *ptr;
{
   ptr->fbdc = NULL;
   ptr->bhandle = NULL;
   if (ptr->handle != NULL)
   {
     fbcbhRelease(ptr->handle);     
   }
   ptr->handle = NULL;
   ptr->size = 0;
   free(ptr); // !!! Change to use Ruby memory functions
}

//
// free_fbresult()
//

static void free_fbresult(ptr) FBSQL_Result *ptr;
{
    fbcmdRelease(ptr->meta);
    free(ptr); // !!! Change to use Ruby memory functions
}

#pragma mark -- private helper functions

//
// get_fbconn()
//

static FBSQL_Connect* get_fbconn(obj) VALUE obj;
{
   FBSQL_Connect *conn = NULL;

   Data_Get_Struct(obj, FBSQL_Connect, conn);
   if (conn == NULL)
   {
     rb_raise(rb_cFBError, "closed connection");
   }

   return conn;
}

//
// get_fbresult()
//

static FBSQL_Result* get_fbresult(obj) VALUE obj;
{
   FBSQL_Result *result = NULL;

   Data_Get_Struct(obj, FBSQL_Result, result);
   if (result == NULL)
   {
     rb_raise(rb_cFBError, "no result available");
   }

   return result;
}

//
// checkMetaData()
//

static VALUE checkMetaData(conn, meta) FBCDatabaseConnection* conn; FBCMetaData* meta;
{
   int result = 1;

   if (meta == NULL)
   {
      rb_raise(rb_cFBError, "Connection to database server was lost.");
      result = 0;
   }
   else if (fbcmdErrorsFound(meta))
   {
      FBCErrorMetaData* emd = fbcdcErrorMetaData(conn, meta);
      char*             emg = fbcemdAllErrorMessages(emd);
      
      if (emg != NULL)
      {
        rb_raise(rb_cFBError, emg);
      }
      else
      {
        rb_raise(rb_cFBError, "No message");
      }

      free(emg); // !!! Use ruby memory functions
      fbcemdRelease(emd);
      result = 0;
   }
   
   return result;
}

// FBResult helper functions

//
// fetch_fbresult()
//

static int fetch_fbresult(FBSQL_Result *result, int row_index, int column_index, VALUE *r)
{
   char* value = NULL;
   int length = 0;

   if (result->meta == NULL)
   {
     rb_raise(rb_cFBError, "No result to fetch.");
   }

   if (row_index < 0)
   {
     rb_raise(rb_cFBError, "Invalid row number.");
   }
      
   if (result->rawData == NULL)
   {
      result->rawData = fbcdcFetch(result->fbdc, FETCH_SIZE, result->fetchHandle);

      if (result->rawData == NULL)
      {
        return -1;
      }
      
      if (result->rowHandler != NULL)
      {
         fbcrhRelease(result->rowHandler);
      }

      result->rowHandler = fbcrhInitWith(result->rawData, result->md);
      result->currentRow = -1;

      if (result->rowHandler == NULL)
      {
         return -1;
      }
   }

   if (result->rowIndex != row_index)
   {
      result->rowIndex = row_index;
      result->currentRow++;
      result->row = (FBCRow *) fbcrhRowAtIndex(result->rowHandler, result->currentRow);
   }

   if (result->row == NULL)
   {
      return -1;
   }

   fetch_convert_value(result, column_index, r);
   
   return 1;
}

//
// fetch_next_row()
//

static int fetch_next_row(FBSQL_Result *result, VALUE *row)
{
   VALUE value = 0;
   int i = 0;

   if (!result->meta)
   {
     rb_raise(rb_cFBError, "No result to fetch.");
   }

   result->row = (FBCRow*) fbcmdFetchRow(result->meta);

   if (!result->row)
   {
      return -1;
   }
   
   for (i=0; i<result->cols; i++)
   {
      fetch_convert_value(result, i, &value);
      rb_ary_push(*row, value);
   }
   
   return 1;
}

//
// fetch_convert_value()
//

static void fetch_convert_value(FBSQL_Result *result, int column_index, VALUE *r)
{
   char* value = NULL;
   const FBCDatatypeMetaData *dtmd = fbcmdDatatypeMetaDataAtIndex(result->md, column_index);
   unsigned dtc = fbcdmdDatatypeCode(dtmd);
   int length = 0;

   if (result->row[column_index] == NULL)
   {
      *r = Qnil;
      return;
   }
   else
   {
      switch(dtc)
      {
         case FB_Boolean:
            switch(result->row[column_index]->boolean)
            {
               case 0:  *r = Qfalse;  break;
               case 1:  *r = Qtrue;   break;
               default: *r = Qnil;    break;
            }
            return;
            
         case FB_PrimaryKey: case FB_Integer:
            *r = INT2NUM(result->row[column_index]->integer);
            return;
            
         case FB_TinyInteger:
            *r = INT2FIX(result->row[column_index]->tinyInteger);
            return;
            
         case FB_SmallInteger:
            *r = INT2FIX(result->row[column_index]->shortInteger);
            return;
            
         case FB_LongInteger:
            *r = LL2NUM(result->row[column_index]->longInteger);
            return;
            
         case FB_Numeric: case FB_Decimal:
         case FB_Float: case FB_Real: case FB_Double:
            *r = rb_float_new(result->row[column_index]->numeric);
            return;
            
         case FB_Character:
         case FB_VCharacter:
            *r = rb_str_new2((char*) result->row[column_index]);
            return;
            
         case FB_Bit:
         case FB_VBit:
         {
            const FBCColumnMetaData* clmd  =  fbcmdColumnMetaDataAtIndex(result->md, column_index);
            const FBCBitValue ptr = result->row[column_index]->bit;
            unsigned nBits = ptr.size * 8;

            if (dtc == FB_Bit) nBits = fbcdmdLength(fbccmdDatatype(clmd));
            
            if (nBits % 8 == 0)
            {
                *r = rb_tainted_str_new((char *)ptr.bytes,ptr.size);
                return;                
            }
            else
            {
               unsigned i = 0;
               unsigned int l = nBits;
               length = l+3+1;
               value = malloc(length); // !!! memory leak?
               value[0] = 'B';
               value[1] = '\'';
               for (i = 0; i < nBits; i++)
               {
                  int bit = 0;
                  if (i/8 < ptr.size) bit = ptr.bytes[i/8] & (1<<(7-(i%8)));
                  value[i*2+2] = bit?'1':'0';
               }
               value[i*2+2] = '\'';
               value[i*2+3] = 0;
            }
            break;
         }
         case FB_BLOB:
         case FB_CLOB:
         {
            unsigned char* bytes = (unsigned char*) result->row[column_index];
            FBSQL_LOB *lob = malloc(sizeof(FBSQL_LOB));

            lob->type = dtc;
            lob->fbdc = result->fbdc;
            lob->bhandle = strdup((char *)&bytes[1]);
            lob->handle = fbcbhInitWithHandle(lob->bhandle);
            lob->size = fbcbhBlobSize(lob->handle);

            *r = Data_Wrap_Struct(rb_cFBLOB, 0, free_fblob, lob);
            return;
         }
         case FB_Date:
         case FB_Time:
         case FB_TimeTZ:
         case FB_Timestamp:
         case FB_TimestampTZ:
         {
            value = strdup((char*) result->row[column_index]);
            break;
         }
         case FB_YearMonth:
         {
            value = "YearMonth";
            break;
         }
         case FB_DayTime:
         {
            value = "DayTime";
            break;
         }
         default:
            rb_raise(rb_cFBError, "Undefined column type.");
      }
   }
   
   *r = rb_tainted_str_new2(value);
   
   return;
}

#pragma mark --- Ruby method definitions ---

#pragma mark ---  FBSQL_Connect methods ---

//
// FBSQL_Connect.new
// FBSQL_Connect.connect
// FBSQL_Connect.setdb
// FBSQL_Connect.setdblogin
// !!! Why 4 methods?

static VALUE fbconn_connect(argc, argv, fbconn) int argc; VALUE *argv; VALUE fbconn;
{
   VALUE arg[7];
   FBSQL_Connect *conn = malloc(sizeof(FBSQL_Connect)); // !!! Use ruby memory functions
   char *session_name = NULL;
   
   conn->port = -1;
   conn->fbeh = NULL;
   rb_scan_args(argc, argv, "07", &arg[0], &arg[1], &arg[2], &arg[3], &arg[4], &arg[5], &arg[6]);

   if (!NIL_P(arg[0]))
   {
      Check_Type(arg[0], T_STRING);
      conn->host = StringValuePtr(arg[0]);
   }
   else
   {
      conn->host = "localhost";
   }
   if (!NIL_P(arg[1]))
   {
      conn->port = NUM2INT(arg[1]);
   }
   if (!NIL_P(arg[2]))
   {
      Check_Type(arg[2], T_STRING);
      conn->database = StringValuePtr(arg[2]);
   }
   if (!NIL_P(arg[3]))
   {
      Check_Type(arg[3], T_STRING);
      conn->user = StringValuePtr(arg[3]);
   }
   else
   {
      conn->user = "";
   }
   if (!NIL_P(arg[4]))
   {
      Check_Type(arg[4], T_STRING);
      conn->password = StringValuePtr(arg[4]);
   }
   else
   {
      conn->password = "";
   }
   
   if (!NIL_P(arg[5]))
   {
      Check_Type(arg[5], T_STRING);
      conn->databasePassword = StringValuePtr(arg[5]);
   }
   else
   {
      conn->databasePassword = "";
   }
   if (!NIL_P(arg[6]))
   {
      Check_Type(arg[6], T_STRING);
      session_name = StringValuePtr(arg[6]);
   }
   else
   {
       session_name = "ruby";
   }

   fbcInitialize();
   
   if (conn->port!=-1)
   {
      conn->fbdc = fbcdcConnectToDatabaseUsingPort(conn->host, conn->port, conn->databasePassword);
   }
   else
   {
      conn->fbdc = fbcdcConnectToDatabase(conn->database, conn->host, conn->databasePassword);
   }

   if (conn->fbdc == NULL)
   {
      rb_raise(rb_cFBError, fbcdcClassErrorMessage());
   }
     
   conn->meta = fbcdcCreateSession(conn->fbdc, session_name, conn->user, conn->password, "system_user");

   if (fbcmdErrorsFound(conn->meta) == T_TRUE)
   {
      FBCErrorMetaData* emd = fbcdcErrorMetaData(conn->fbdc, conn->meta);
      char* msgs = fbcemdAllErrorMessages(emd);

      rb_raise(rb_cFBError, msgs);
      fbcemdRelease(emd);
      free(msgs);

      fbcmdRelease(conn->meta);
      conn->meta = NULL;

      fbcdcClose(conn->fbdc);
      fbcdcRelease(conn->fbdc);
      conn->fbdc = NULL;

      return 0;
   }
   fbcmdRelease(conn->meta);
   conn->meta = NULL;

   return Data_Wrap_Struct(fbconn, 0, free_fbconn, conn);
}

//
// FBSQL_Connect#close
//

static VALUE fbconn_close(obj) VALUE obj;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   
   if (!fbcdcConnected(conn->fbdc))
   {
     rb_raise(rb_cFBError, "connection already closed.");
   }

   if (conn->meta)
   {
     fbcmdRelease(conn->meta);
   }
   conn->meta = NULL;

   if (conn->fbdc)
   {
      fbcdcClose(conn->fbdc);
      fbcdcRelease(conn->fbdc);
   }
   conn->fbdc = NULL;

   if (conn->fbeh)
   {
     fbcehRelease(conn->fbeh);
   }
   conn->fbeh = NULL;

   if (conn->host)
   {
      conn->host = NULL;
   }
   if (conn->database)
   {
      conn->database = NULL;
   }
   if (conn->user)
   {
      conn->user = NULL;
   }
   if (conn->password)
   {
      conn->password = NULL;
   }
   if (conn->databasePassword)
   {
      conn->databasePassword = NULL;
   }
   
   DATA_PTR(obj) = 0;
   
   return Qnil;
}

//
// FBSQL_Connect#
//

static VALUE fbconn_autocommit(obj, commit) VALUE obj; int commit;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBCMetaData*   md;
   int i = NUM2INT(commit);

   if (conn->fbdc) {
      if (i)
         md = fbcdcExecuteDirectSQL(conn->fbdc,"SET COMMIT TRUE;");
      else
         md = fbcdcExecuteDirectSQL(conn->fbdc,"SET COMMIT FALSE;");

      checkMetaData(conn->fbdc, md);
      if (md)
         fbcmdRelease(md);
   }
   else
      rb_raise(rb_cFBError, "No connection available");
      
   return Qnil;
}

//
// FBSQL_Connect#
//

static VALUE fbconn_database_server_info(obj) VALUE obj;
{
   VALUE ret;
   VALUE result = fbconn_query(obj, rb_tainted_str_new2("VALUES(SERVER_NAME);"));
   fetch_fbresult(get_fbresult(result), 0, 0, &ret);
   fbresult_clear(result);
   
   return ret;
}

//
// FBSQL_Connect#
//

static VALUE fbconn_commit(obj) VALUE obj;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBCMetaData*   md;

   md = fbcdcCommit(conn->fbdc);
   checkMetaData(conn->fbdc, md);
   return Qnil;
}

//
// FBSQL_Connect#
//
   
static VALUE fbconn_rollback(obj) VALUE obj;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBCMetaData*   md;

   md = fbcdcRollback(conn->fbdc);
   checkMetaData(conn->fbdc, md);
   return Qnil;
}

//
// FBSQL_Connect#
//

static VALUE fbconn_query(obj, str) VALUE obj, str;
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBSQL_Result *result = malloc(sizeof(FBSQL_Result));
   FBCMetaData *meta = NULL;

   result->fbdc = conn->fbdc;
   
   int status = FRONTBASE_COMMAND_OK;
   const char *msg = NULL, *type = NULL;
   char *sql = NULL, *sqlCmd = NULL;
   unsigned len = 0;

   Check_Type(str, T_STRING);

   sql = StringValuePtr(str);
   len = strlen(sql);

   sqlCmd = malloc(len + 1 + 1);

   sprintf(sqlCmd, "%s", sql);
   if (sql[len-1] != ';')
      strcat(sqlCmd, ";");
   
   meta = fbcdcExecuteDirectSQL(conn->fbdc, sqlCmd);

   checkMetaData(conn->fbdc, meta);

   result->currentResult = 0;
   result->resultCount = 1;
   
   if (fbcmdHasMetaDataArray(meta))
   {
      result->resultCount = fbcmdMetaDataArrayCount(meta);
      result->md = (FBCMetaData*) fbcmdMetaDataAtIndex(meta, 0);
      result->meta = meta;
   }
   else
   {
      result->md = meta;
      result->meta = meta;
   }
   
   type = fbcmdStatementType(result->md);
   
   if (type != NULL && strcmp("SELECT", type) == 0)
   {
      status = FRONTBASE_ROWS_OK;
   }
   else if(type != NULL && strcmp("UNIQUE", type) == 0)
   {
      status = FRONTBASE_UNIQUE_OK;
   }

   switch (status)
   {
      case FRONTBASE_COMMAND_OK:
      case FRONTBASE_ROWS_OK:
      case FRONTBASE_UNIQUE_OK:
         result->row = NULL;
         result->rawData = NULL;
         result->rowHandler = NULL;
         result->fetchHandle = fbcmdFetchHandle(result->meta);
         result->rows = fbcmdRowCount(result->meta);
         result->cols = fbcmdColumnCount(result->meta);
         result->rowIndex = -1;
         return Data_Wrap_Struct(rb_cFBResult, 0, free_fbresult, result);

      default:
         msg = fbcdcErrorMessage(conn->fbdc);
         break;
   }

   fbcmdRelease(result->meta);
   rb_raise(rb_cFBError, msg);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_exec(obj, str) VALUE obj, str;
{
   VALUE result = fbconn_query(obj, str);
   fbresult_clear(result);
   return result;
}

//
// FBSQL_Connect#
//

static VALUE fbconn_host(obj) VALUE obj;
{
   const char *host = fbcdcHostName(get_fbconn(obj)->fbdc);
   if (!host) return Qnil;
   return rb_tainted_str_new2(host);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_db(obj) VALUE obj;
{
   const char *db = fbcdcDatabaseName(get_fbconn(obj)->fbdc);
   if (!db) return Qnil;
   return rb_tainted_str_new2(db);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_user(obj) VALUE obj;
{
   return rb_tainted_str_new2(get_fbconn(obj)->user);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_status(obj) VALUE obj;
{
   Bool status = fbcdcConnected(get_fbconn(obj)->fbdc);

   return INT2NUM(status ? 1 : 0);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_error(obj) VALUE obj;
{
   const char *error = fbcdcErrorMessage(get_fbconn(obj)->fbdc);
   if (!error) return Qnil;
   return rb_tainted_str_new2(error);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_create_blob(VALUE obj, VALUE data)
{
   int size;

   FBSQL_Connect *conn = get_fbconn(obj);
   FBSQL_LOB *lob = malloc(sizeof(FBSQL_LOB));
   size = RSTRING_LEN(data);

   lob->type = FB_BLOB;
   lob->fbdc = conn->fbdc;
   lob->bhandle = NULL;
   lob->handle = fbcdcWriteBLOB(conn->fbdc, RSTRING_PTR(data), size);
   lob->size = size;

   return Data_Wrap_Struct(rb_cFBLOB, 0, free_fblob, lob);
}

//
// FBSQL_Connect#
//

static VALUE fbconn_create_clob(VALUE obj, VALUE data)
{
   FBSQL_Connect *conn = get_fbconn(obj);
   FBSQL_LOB *lob = malloc(sizeof(FBSQL_LOB));

   lob->type = FB_CLOB;
   lob->fbdc = conn->fbdc;
   lob->bhandle = NULL;
   lob->handle = fbcdcWriteCLOB(conn->fbdc, RSTRING_PTR(data));
   lob->size = RSTRING_LEN(data);

   return Data_Wrap_Struct(rb_cFBLOB, 0, free_fblob, lob);
}


#pragma mark ---  FBResult methods ---

//
// FBResult#status
//

static VALUE fbresult_status(obj) VALUE obj;
{
   FBSQL_Result *result;
   int status = FRONTBASE_COMMAND_OK;
   char *type;

   result = get_fbresult(obj);

   if (fbcmdErrorsFound(result->meta) == T_TRUE)
   {
     return -1;
   }
   
   type = fbcmdStatementType(result->meta);

   if (type != NULL && strcmp("SELECT", type) == 0)
   {
      status = FRONTBASE_ROWS_OK;
   }
   else if (type != NULL && strcmp("UNIQUE", type) == 0)
   {
      status = FRONTBASE_UNIQUE_OK;
   }

   return INT2NUM(status);
}

//
// FBResult#result
//

static VALUE fbresult_result(obj) VALUE obj;
{
   FBSQL_Result *result;
   VALUE ary, row;
   int i;
   
   result = get_fbresult(obj);
   ary = rb_ary_new2(result->rows);

   if (fbcmdFetchHandle(result->meta) == NULL)
   {
     return ary;
   }
   
   while (1)
   {
      VALUE row = rb_ary_new2(result->cols);
      i = fetch_next_row(result, &row);
      if (i != -1)
      {
        rb_ary_push(ary, row);
      }
      else
      {
        return ary;
      }
   }

   return ary;
}

//
// FBResult#each
//

static VALUE fbresult_each(obj) VALUE obj;
{
   FBSQL_Result *result;
   int i, j;
   VALUE row;

   result = get_fbresult(obj);
   
   if (fbcmdFetchHandle(result->meta) == NULL)
   {
     return Qnil;
   }

   while (1)
   {
      VALUE row = rb_ary_new2(result->cols);
      i = fetch_next_row(result, &row);
      if (i != -1)
      {
        rb_yield(row);
      }
      else
      {
        return Qnil;
      }
   }
   
   return Qnil;
}

//
// FBResult#[]
//

static VALUE fbresult_aref(argc, argv, obj) int argc; VALUE *argv; VALUE obj;
{
   FBSQL_Result *result;
   VALUE a1, a2, val, value;
   int i, j;

   result = get_fbresult(obj);

   switch (rb_scan_args(argc, argv, "11", &a1, &a2))
   {
      case 1:
         i = NUM2INT(a1);
         if( i >= result->rows ) return Qnil;

            val = rb_ary_new();
         for (j=0; j<result->cols; j++)
         {
            fetch_fbresult(result, i, j, &value);
            rb_ary_push(val, value);
         }
            return val;

      case 2:
         i = NUM2INT(a1);
         if( i >= result->rows ) return Qnil;
         j = NUM2INT(a2);
         if( j >= result->cols ) return Qnil;

         fetch_fbresult(result, i, j, &value);
         return value;

      default:
         return Qnil;   /* not reached */
   }
}

//
// FBResult#columns
//

static VALUE fbresult_columns(obj) VALUE obj;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;
   VALUE ary;
   int i;

   result = get_fbresult(obj);
   ary = rb_ary_new2(result->cols);
   
   for (i=0;i<result->cols;i++)
   {
      column_meta = fbcmdColumnMetaDataAtIndex(result->meta, i);
      rb_ary_push(ary, rb_tainted_str_new2(fbccmdLabelName(column_meta)));
   }
   
   return ary;
}

//
// FBResult#num_rows
//

static VALUE fbresult_num_rows(obj) VALUE obj;
{
   return INT2NUM(get_fbresult(obj)->rows);
}

//
// FBResult#row_index
//
// added by Eric Ocean
//

static VALUE fbresult_row_index(obj) VALUE obj;
{
  long retValue = 0;
  FBSQL_Result *res = get_fbresult(obj);
  FBCMetaData *md = res->meta;
  // int fbcmdRowCount(const FBCMetaData* self);
  retValue = fbcmdRowIndex(md);
  
   return INT2NUM( retValue );
}

//
// FBResult#table_name
//
// added by Eric Ocean
//

static VALUE fbresult_table_name(obj) VALUE obj;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;

   result = get_fbresult(obj);
   column_meta = fbcmdColumnMetaDataAtIndex(result->meta, 0);

   return rb_tainted_str_new2(fbccmdTableName(column_meta));
}

//
// FBResult#num_cols
//

static VALUE fbresult_num_cols(obj) VALUE obj;
{
   return INT2NUM(get_fbresult(obj)->cols);
}

//
// FBResult#column_name
//

static VALUE fbresult_column_name(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;
   int i = NUM2INT(index);

   result = get_fbresult(obj);

   if (i < 0 || i >= result->cols)
   {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   column_meta = fbcmdColumnMetaDataAtIndex(result->meta, i);

   return rb_tainted_str_new2(fbccmdLabelName(column_meta));
}

//
// FBResult#column_type
//

static VALUE fbresult_column_type(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;
   
   int i = NUM2INT(index);
   int type;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);
   
   if (i < 0 || i >= result->cols)
   {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta)
   {
      type = fbcdmdDatatypeCode(datatype_meta);
   }

   return INT2NUM(type);
}

//
// FBResult#column_length
//

static VALUE fbresult_column_length(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;

   int i = NUM2INT(index);
   int size;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);
   
   if (i < 0 || i >= result->cols)
   {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta)
   {
      size = fbcdmdLength(datatype_meta);
   }
   
   return INT2NUM(size);
}

//
// FBResult#column_precision
//

static VALUE fbresult_column_precision(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;

   int i = NUM2INT(index);
   int size;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);

   if (i < 0 || i >= result->cols)
   {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta)
   {
      size = fbcdmdPrecision(datatype_meta);
   }

   return INT2NUM(size);
}

//
// FBResult#column_scale
//

static VALUE fbresult_column_scale(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCDatatypeMetaData *datatype_meta;

   int i = NUM2INT(index);
   int size;

   result = get_fbresult(obj);
   datatype_meta = fbcmdDatatypeMetaDataAtIndex(result->meta, i);

   if (i < 0 || i >= result->cols)
   {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   if (datatype_meta)
   {
      size = fbcdmdScale(datatype_meta);
   }

   return INT2NUM(size);
}

//
// FBResult#column_isnullable
//

static VALUE fbresult_column_isnullable(obj, index) VALUE obj, index;
{
   FBSQL_Result *result;
   const FBCColumnMetaData *column_meta;
   int i = NUM2INT(index);

   result = get_fbresult(obj);

   if (i < 0 || i >= result->cols)
   {
      rb_raise(rb_eArgError,"invalid column number %d", i);
   }

   column_meta = fbcmdColumnMetaDataAtIndex(result->meta, i);
   
   return fbccmdIsNullable(column_meta)? Qtrue : Qfalse;
}

//
// FBResult#clear
// FBResult#close
//

static VALUE fbresult_clear(obj) VALUE obj;
{
   FBSQL_Result *result = get_fbresult(obj);

   if (result->meta)
   {
      fbcmdRelease(result->meta);
   }
   result->meta = NULL;
   result->md = NULL;

   if (result->fbdc)
   {
      result->fbdc = NULL;
   }

   if (result->rowHandler != NULL)
   {
      fbcrhRelease(result->rowHandler);
   }

   result->row = NULL;
   result->rawData = NULL;
   result->fetchHandle = NULL;

   DATA_PTR(obj) = 0;
   
   return Qnil;
}

#pragma mark ---  FBSQL_LOB methods ---

//
// FBBlob#
//

static VALUE fblob_read(obj) VALUE obj;
{
   FBSQL_LOB *lob = get_fblob(obj);

   if (lob->type == FB_BLOB)
   {
     return rb_tainted_str_new((char*) fbcdcReadBLOB(lob->fbdc, lob->handle), lob->size);
   }
   else
   {
     return rb_tainted_str_new((char*) fbcdcReadCLOB(lob->fbdc, lob->handle), lob->size);
   }
}

//
// FBBlob#
//

static VALUE fblob_handle(obj) VALUE obj;
{
   FBSQL_LOB *lob = get_fblob(obj);

   return rb_tainted_str_new2(fbcbhDescription(lob->handle));
}

//
// FBBlob#
//

static VALUE fblob_size(obj) VALUE obj;
{
   FBSQL_LOB *lob = get_fblob(obj);

   return INT2NUM(lob->size);
}

//
// FBBlob#
//

static FBSQL_LOB* get_fblob(obj) VALUE obj;
{
   FBSQL_LOB *lob;

   Data_Get_Struct(obj, FBSQL_LOB, lob);
   if (lob == 0)
   {
     rb_raise(rb_cFBError, "no blob available");
   }

   return lob;
}

#pragma mark --- Ruby initialization ---

void Init_frontbase()
{
   rb_cFBConn   = rb_define_class("FBSQL_Connect", rb_cObject);
   rb_cFBResult = rb_define_class("FBSQL_Result", rb_cObject);
   rb_cFBLOB    = rb_define_class("FBSQL_LOB", rb_cObject);
   rb_cFBError  = rb_define_class("FBError", rb_eStandardError);

   //
   // FBSQL_LOB
   //
   rb_define_method(rb_cFBLOB, "read", fblob_read, 0);
   rb_define_method(rb_cFBLOB, "handle", fblob_handle, 0);
   rb_define_method(rb_cFBLOB, "size", fblob_size, 0);

   //
   // FBSQL_Connect
   //
   
   // Class methods
   rb_define_singleton_method(rb_cFBConn, "new", fbconn_connect, -1);
   rb_define_singleton_method(rb_cFBConn, "connect", fbconn_connect, -1);
   rb_define_singleton_method(rb_cFBConn, "setdb", fbconn_connect, -1);
   rb_define_singleton_method(rb_cFBConn, "setdblogin", fbconn_connect, -1);

   // Constants
   rb_define_const(rb_cFBConn, "NO_CONNECTION", INT2FIX(FB_ERR_NO_CONNECTION));
   rb_define_const(rb_cFBConn, "FB_BINDINGS_VERSION", rb_str_new2(RUBY_BINDINGS_VERSION));

   // Instance methods
   rb_define_method(rb_cFBConn, "create_blob", fbconn_create_blob, 1);
   rb_define_method(rb_cFBConn, "create_clob", fbconn_create_clob, 1);

   rb_define_method(rb_cFBConn, "database_server_info", fbconn_database_server_info, 0);
   rb_define_method(rb_cFBConn, "autocommit", fbconn_autocommit, 1);
   rb_define_method(rb_cFBConn, "commit", fbconn_commit, 0);
   rb_define_method(rb_cFBConn, "rollback", fbconn_rollback, 0);
   rb_define_method(rb_cFBConn, "db", fbconn_db, 0);
   rb_define_method(rb_cFBConn, "host", fbconn_host, 0);
   rb_define_method(rb_cFBConn, "status", fbconn_status, 0);
   rb_define_method(rb_cFBConn, "error", fbconn_error, 0);
   rb_define_method(rb_cFBConn, "close", fbconn_close, 0);
   rb_define_alias(rb_cFBConn, "finish", "close");
   rb_define_method(rb_cFBConn, "user", fbconn_user, 0);

   rb_define_method(rb_cFBConn, "exec", fbconn_exec, 1);
   rb_define_method(rb_cFBConn, "query", fbconn_query, 1);
   
   rb_define_const(rb_cFBConn, "COMMAND_OK", INT2FIX(FRONTBASE_COMMAND_OK));
   rb_define_const(rb_cFBConn, "ROWS_OK", INT2FIX(FRONTBASE_ROWS_OK));
   rb_define_const(rb_cFBConn, "UNIQUE_OK", INT2FIX(FRONTBASE_UNIQUE_OK));

   rb_define_const(rb_cFBConn, "FB_Undecided", INT2FIX(FB_Undecided));
   rb_define_const(rb_cFBConn, "FB_PrimaryKey", INT2FIX(FB_PrimaryKey));
   rb_define_const(rb_cFBConn, "FB_Boolean", INT2FIX(FB_Boolean));
   rb_define_const(rb_cFBConn, "FB_Integer", INT2FIX(FB_Integer));
   rb_define_const(rb_cFBConn, "FB_SmallInteger", INT2FIX(FB_SmallInteger));
   rb_define_const(rb_cFBConn, "FB_Float", INT2FIX(FB_Float));
   rb_define_const(rb_cFBConn, "FB_Real", INT2FIX(FB_Real));
   rb_define_const(rb_cFBConn, "FB_Double", INT2FIX(FB_Double));
   rb_define_const(rb_cFBConn, "FB_Numeric", INT2FIX(FB_Numeric));
   rb_define_const(rb_cFBConn, "FB_Decimal", INT2FIX(FB_Decimal));
   rb_define_const(rb_cFBConn, "FB_Character", INT2FIX(FB_Character));
   rb_define_const(rb_cFBConn, "FB_VCharacter", INT2FIX(FB_VCharacter));
   rb_define_const(rb_cFBConn, "FB_Bit", INT2FIX(FB_Bit));
   rb_define_const(rb_cFBConn, "FB_VBit", INT2FIX(FB_VBit));
   rb_define_const(rb_cFBConn, "FB_Date", INT2FIX(FB_Date));
   rb_define_const(rb_cFBConn, "FB_Time", INT2FIX(FB_Time));
   rb_define_const(rb_cFBConn, "FB_TimeTZ", INT2FIX(FB_TimeTZ));
   rb_define_const(rb_cFBConn, "FB_Timestamp", INT2FIX(FB_Timestamp));
   rb_define_const(rb_cFBConn, "FB_TimestampTZ", INT2FIX(FB_TimestampTZ));
   rb_define_const(rb_cFBConn, "FB_YearMonth", INT2FIX(FB_YearMonth));
   rb_define_const(rb_cFBConn, "FB_DayTime", INT2FIX(FB_DayTime));
   rb_define_const(rb_cFBConn, "FB_CLOB", INT2FIX(FB_CLOB));
   rb_define_const(rb_cFBConn, "FB_BLOB", INT2FIX(FB_BLOB));
   rb_define_const(rb_cFBConn, "FB_TinyInteger", INT2FIX(FB_TinyInteger));
   rb_define_const(rb_cFBConn, "FB_LongInteger", INT2FIX(FB_LongInteger));

   rb_include_module(rb_cFBResult, rb_mEnumerable);

   rb_define_const(rb_cFBResult, "COMMAND_OK", INT2FIX(FRONTBASE_COMMAND_OK));
   rb_define_const(rb_cFBResult, "ROWS_OK", INT2FIX(FRONTBASE_ROWS_OK));
   rb_define_const(rb_cFBResult, "UNIQUE_OK", INT2FIX(FRONTBASE_UNIQUE_OK));

   rb_define_method(rb_cFBResult, "status", fbresult_status, 0);
   rb_define_method(rb_cFBResult, "result", fbresult_result, 0);
   rb_define_method(rb_cFBResult, "each", fbresult_each, 0);
   rb_define_method(rb_cFBResult, "[]", fbresult_aref, -1);
   rb_define_method(rb_cFBResult, "columns", fbresult_columns, 0);
   rb_define_method(rb_cFBResult, "num_rows", fbresult_num_rows, 0);
   
   // added by Eric Ocean
   rb_define_method(rb_cFBResult, "row_index", fbresult_row_index, 0);
   rb_define_method(rb_cFBResult, "table_name", fbresult_table_name, 0); // not sure if the '0' is right

   rb_define_method(rb_cFBResult, "num_cols", fbresult_num_cols, 0);
   rb_define_method(rb_cFBResult, "column_name", fbresult_column_name, 1);
   rb_define_method(rb_cFBResult, "column_type", fbresult_column_type, 1);
   rb_define_method(rb_cFBResult, "column_length", fbresult_column_length, 1);
   rb_define_method(rb_cFBResult, "column_precision", fbresult_column_precision, 1);
   rb_define_method(rb_cFBResult, "column_scale", fbresult_column_scale, 1);
   rb_define_method(rb_cFBResult, "column_isnullable", fbresult_column_isnullable, 1);
   rb_define_method(rb_cFBResult, "clear", fbresult_clear, 0);
   rb_define_method(rb_cFBResult, "close", fbresult_clear, 0);
}
