#include <iostream>
#include <string>
#include <tuple>
#include <fstream>
#include <sys/time.h>
#include "highway_cover_labelling.h"

using namespace std;

double GetCurrentTimeMS() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000. + tv.tv_usec / 1000.;
}

tuple<int*, int*, int> LoadQueries(const char *filename) {
  int *srcs = new int[100];
  int *dests = new int[100];
  int num_queries = 0;

  std::ifstream ifs(filename);
  if (ifs.is_open()) {

    int v, w; std::string query;
    while (getline(ifs, query)){
      std::istringstream iss(query);
      iss >> v >> w;
      if (v != w) {
        srcs[num_queries] = v;
        dests[num_queries] = w;
        num_queries++;
      }
    }
    ifs.close();
  } else {
    std::cout << "Unable to open file" << std::endl;
  }
  return std::make_tuple(srcs, dests, num_queries);
}

int main(int argc, char **argv) {

	int k = atoi(argv[2]);
	HighwayLabelling *hl = new HighwayLabelling(argv[1], k);
	hl->LoadIndex(argv[3]);
	double time = GetCurrentTimeMS();
    int topk[k];
	hl->SelectLandmarks_HD(topk);
	hl->RemoveLandmarks(topk);
	double index_time = GetCurrentTimeMS() - time;

    // load queries
    int *queries_u;
    int *queries_v;
    int num_queries = 0;
    std::string fileName = argv[4];
    tie(queries_u, queries_v, num_queries) = LoadQueries(fileName.c_str());
    // start query answering
    int s, t;
    printf("Num Queries: %d\n", num_queries);
    for (int i = 0; i < num_queries; i++) {
        try {
            s = queries_u[i];
            t = queries_v[i];
            time = GetCurrentTimeMS();
            int distance = (int) hl->QueryDistance(s, t);
            time = GetCurrentTimeMS() - time;
            printf( "Path=%s index_time=%0.3lfms query_time=%0.3lfms u=%d v=%d d=%d\n", argv[3], index_time, time, s, t, distance );
        } catch(...) {
            printf( "UNABLE TO ANSWER: Path=%s u=%d v=%d\n", argv[3], s, t );
        }
    }
	exit(EXIT_SUCCESS);
}
