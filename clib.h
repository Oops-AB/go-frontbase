#include <FBCAccess/FBCAccess.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

FBCDatabaseConnection *GoFBOpen(const char *url);

void GoFBClose(FBCDatabaseConnection *connection);

int GoFBPing(FBCDatabaseConnection *connection);

FBCColumn *GoFBColumnAtIndex(FBCRow *row, unsigned int i);

uint8_t GoFBColumnValueBool(FBCColumn *col);
int8_t GoFBColumnValueTinyInt(FBCColumn *col);
int16_t GoFBColumnValueSmallInt(FBCColumn *col);
int32_t GoFBColumnValueInt(FBCColumn *col);
int64_t GoFBColumnValueLongInt(FBCColumn *col);
double GoFBColumnValueDouble(FBCColumn *col);
double GoFBColumnValueDecimal(FBCColumn *col);
char *GoFBColumnValueChar(FBCColumn *col);

unsigned char *GoFBColumnValueBit(FBCColumn *col);
int GoFBColumnSizeBit(FBCColumn *col);

struct GoFBTimestampValue {
  int64_t secs;
  int64_t nsecs;
};

void GoFBColumnValueTimestamp(FBCColumn *col, struct GoFBTimestampValue *res);
