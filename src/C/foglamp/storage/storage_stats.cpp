#include <storage_stats.h>
#include <string>
#include <sstream>

using namespace std;

StorageStats::StorageStats() : commonInsert(0), commonSimpleQuery(0),
				commonQuery(0), commonUpdate(0), commonDelete(0),
				readingAppend(0), readingFetch(0),
				readingQuery(0), readingPurge(0)
{
}

void StorageStats::asJSON(string& json)
{
ostringstream convert;   // stream used for the conversion

	convert << "{ \"commonInsert\" : " << commonInsert << ",";
	convert << " \"commonSimpleQuery\" : " << commonSimpleQuery << ",";
	convert << " \"commonQuery\" : " << commonQuery << ",";
	convert << " \"commonUpdate\" : " << commonUpdate << ",";
	convert << " \"commonDelete\" : " << commonDelete << ",";
	convert << " \"readingAppend\" : " << readingAppend << ",";
	convert << " \"readingFetch\" : " << readingFetch << ",";
	convert << " \"readingQuery\" : " << readingQuery << ",";
	convert << " \"readingPurge\" : " << readingPurge << " }";

	json = convert.str();
}
