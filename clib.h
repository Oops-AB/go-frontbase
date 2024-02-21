#include <FBCAccess/FBCAccess.h>
#include <stdio.h>
#include <stdlib.h>

FBCDatabaseConnection *MyFBOpen(const char *url);

void MyFBClose(FBCDatabaseConnection *connection);

int MyFBPing(FBCDatabaseConnection *connection);

FBCColumn *MyFBColumnAtIndex(FBCRow *row, unsigned int i);

uint8_t MyFBColumnValueBool(FBCColumn *col);
int8_t MyFBColumnValueTinyInt(FBCColumn *col);
int16_t MyFBColumnValueSmallInt(FBCColumn *col);
int32_t MyFBColumnValueInt(FBCColumn *col);
int64_t MyFBColumnValueLongInt(FBCColumn *col);
double MyFBColumnValueDouble(FBCColumn *col);
char *MyFBColumnValueChar(FBCColumn *col);

unsigned char *MyFBColumnValueBit(FBCColumn *col);
int MyFBColumnSizeBit(FBCColumn *col);

struct MyFBTimestampValue {
  int64_t secs;
  int64_t nsecs;
};

void MyFBColumnValueTimestamp(FBCColumn *col, struct MyFBTimestampValue *res);
