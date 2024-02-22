#include <FBCAccess/FBCAccess.h>
#include <stdio.h>
#include <stdlib.h>
#include "clib.h"

FBCDatabaseConnection *GoFBOpen(const char *url) {
	FBCMetaData *md = fbcdcConnectToURL(url,"","_system","","sid");

	if (fbcmdErrorsFound(md)) {
		fprintf(stderr, "%s: open failed\n", url);
		fbcmdRelease(md);
		return NULL;
	}

	FBCDatabaseConnection *connection = fbcdcRetain(fbcmdDatabaseConnection(md));
	fbcmdRelease(md);

	fbcdcSetFormatResult(connection, 0);
	return connection;
}

void GoFBClose(FBCDatabaseConnection *connection) {
	if (connection == NULL) return;

	fbcdcClose(connection);
	fbcdcRelease(connection);
}

int GoFBPing(FBCDatabaseConnection *connection) {
	if (connection == NULL) return 0;

	const char *url = fbcdcURL(connection);
	if (url == NULL) url = "???";
	fprintf(stderr, "%s: ping\n", url);

	return fbcdcConnected(connection);
}

FBCColumn *GoFBColumnAtIndex(FBCRow *row, unsigned int i) {
	return row[i];
}

uint8_t GoFBColumnValueBool(FBCColumn *col) {
	return col->boolean;
}

int8_t GoFBColumnValueTinyInt(FBCColumn *col) {
	return col->tinyInteger;
}

int16_t GoFBColumnValueSmallInt(FBCColumn *col) {
	return col->tinyInteger;
}

int32_t GoFBColumnValueInt(FBCColumn *col) {
	return col->integer;
}

int64_t GoFBColumnValueLongInt(FBCColumn *col) {
	return col->longInteger;
}

double GoFBColumnValueDouble(FBCColumn *col) {
	return col->real;
}

char *GoFBColumnValueChar(FBCColumn *col) {
	return col->character;
}

unsigned char *GoFBColumnValueBit(FBCColumn *col) {
	return col->bit.bytes;
}

int GoFBColumnSizeBit(FBCColumn *col) {
	return col->bit.size;
}

void GoFBColumnValueTimestamp(FBCColumn *col, struct GoFBTimestampValue *res) {
	if (res == NULL) return;
	double secs = col->rawTimestamp.seconds;

	res->secs = (int64_t)secs;

	double fraction = secs - res->secs;
	res->nsecs = (int64_t)(fraction * 1000000000.0);

	res->secs += 978307200;

	if (res->nsecs < 0) {
		res->nsecs = 1000000000 + res->nsecs;
	}
}
