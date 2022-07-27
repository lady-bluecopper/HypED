#include "CoreTreeLabelling.h"
#include "PLL.h"

void index_pll(string path) {
	PrunedLandmarkLabeling<> pll;
	vector< pair<int,int> > el;

	FILE *fin = fopen( (path + "graph.txt").c_str(), "r" );
	char line[MAXLINE];
	int n = 0, a, b, num_cnt = CoreTree::get_num_cnt(path);
	long long cnt = 0, m = 0;

	printf( "Loading Data...\n" );
	while( fgets( line, MAXLINE, fin ) ) {
		if( !CoreTree::get_edge(line, a, b, num_cnt) ) continue;
		n = max(max(n, a+1), b+1);
		if( (++cnt) % (long long) 10000000 == 0 ) printf( "%lld lines finished\n", cnt );
		++m;
		el.push_back( make_pair(a,b) );
	}
	fclose( fin );
	printf( "n=%d,m=%lld\n", n, m );

	double t = omp_get_wtime();
	pll.ConstructIndex(el);
	pll.StoreIndex((path+"index-pll.bin").c_str());

	t = omp_get_wtime() - t;
	printf( "PLL: t=%0.3lf secs\n", t );
	pll.PrintStatistics();
}

void cross_check(string path, int max_w, int n_pairs) {
	PrunedLandmarkLabeling<> pll;
	pll.LoadIndex((path+"index-pll.bin").c_str());
	int n = pll.GetNumVertices();
	printf( "load_pll: path=%s, num_v=%d, n_pairs=%d\n", path.c_str(), n, n_pairs );

	CoreTree ct(path);
	ct.load_label(max_w);
	//ct.load_graph();
	//ct.decompose(max_w, 1);
	//ct.init_query();

	int n_error = 0;
	for( int i = 0; i < n_pairs; ++i ) {
		int u = rand() % n, v = rand() % n;
		int dis1 = pll.QueryDistance(u,v);
		int dis2 = ct.query(u,v);
		if(dis1 != dis2) {
			++n_error;
			int uu = ct.nid[u], vv = ct.nid[v];
			if(uu<0) uu = -uu-1; else if(uu>=MAXN) uu -= MAXN;
			if(vv<0) vv = -vv-1; else if(vv>=MAXN) vv -= MAXN;
			printf( "rank[u]=%d,rank[v]=%d,rid[u]=%d,rid[v]=%d\n", ct.rank[uu], ct.rank[vv], ct.tree[uu].rid, ct.tree[vv].rid);
			printf( "u=%d v=%d pll_dis=%d ct_dis=%d\n", u, v, dis1, dis2);
		}
	}
	printf( "n_error=%d\n", n_error );

	long long td = 0, n_valid = 0;
	double t = omp_get_wtime();
	for( int i = 0; i < n_pairs; ++i ) {
		int u = rand() % n, v = rand() % n;
		int dis = pll.QueryDistance(u,v);
		if( dis < INT_MAX ) {td += dis; ++n_valid;}
	}
	t = omp_get_wtime() - t;
	printf( "PLL:average query time = %0.3lfmicros, average dis = %0.3lf\n", t*1000000.0/n_pairs, td*1.0/n_valid);

	td = 0, n_valid = 0;
	t = omp_get_wtime();
	for( int i = 0; i < n_pairs; ++i ) {
		int u = rand() % n, v = rand() % n;
		int dis = ct.query(u,v);
		if( dis < INT_MAX ) {td += dis; ++n_valid;}
	}
	t = omp_get_wtime() - t;
	printf( "CT:average query time = %0.3lfmicros, average dis = %0.3lf\n", t*1000000.0/n_pairs, td*1.0/n_valid);

}


void query_dis(string path, int max_w, int n_pairs) {
	CoreTree ct(path);
	ct.load_label(max_w);

	int n = ct.n_org;
	printf( "query_dis: path=%s, num_v=%d, n_pairs=%d\n", path.c_str(), n, n_pairs );

	for( int r = 0; r < 5; ++r ) {
		long long td = 0, n_valid = 0;
		int max_dis = 0, min_dis = INT_MAX;
		double t = omp_get_wtime();
		for( int i = 0; i < n_pairs; ++i ) {
			int u = rand() % n, v = rand() % n;
			int dis = ct.query(u,v);
			if( dis < INT_MAX ) {td += dis; ++n_valid; max_dis = max(max_dis, dis); min_dis = min(min_dis, dis);}
		}
		t = omp_get_wtime() - t;
		printf( "round %d: average query time = %0.3lfmicros, n_valid = %d, max dis = %d, min_dis = %d, average dis = %0.3lf\n",
				r+1, t*1000000.0/n_pairs, n_valid, max_dis, min_dis, td*1.0/n_valid);
	}
}


void query_dis(string path, int max_w, int u, int v) {
	CoreTree ct(path);
	ct.load_label(max_w);

	int n = ct.n_org;
	double t = omp_get_wtime();
	int dis = ct.query(u,v);
	if( dis == INT_MAX ) {dis = -1;}
	t = omp_get_wtime() - t;
	printf( "path=%s time=%0.3lfmicros, dis=%d, u=%d, v=%d\n",
			path.c_str(), t*1000000.0, dis, u, v );
}


int main(int argc, char *argv[]) {
	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	if( argc > 1 ) {
		if(strcmp( argv[1], "txt-to-bin" ) == 0)
			CoreTree::create_bin(argv[2],				/*path*/
				argc>3?atoi(argv[3]):1, 				/*rank_threads*/
				argc>4?atoi(argv[4]):RANK_STATIC,		/*rank_method*/
				argc>5?atoi(argv[5]):60, 				/*rank_max_minutes*/
				argc>6?atoi(argv[6]):3					/*max_hops*/
			);
		else if(strcmp(argv[1], "decompose_bp") == 0) {
			CoreTree ct(argv[2]);						/*path*/
			ct.decompose_bp(atoi(argv[3]));				/*n_threads*/
		} else if(strcmp(argv[1], "decompose_tree") == 0) {
			CoreTree ct(argv[2]);						/*path*/
			ct.decompose_tree(atoi(argv[3]),			/*max_w*/
				argc>4?atoi(argv[4]):1					/*n_threads*/
			);
		} else if(strcmp(argv[1], "decompose_core") == 0) {
			CoreTree ct(argv[2]);						/*path*/
			ct.decompose_core(atoi(argv[3]),			/*max_w*/
				argc>4?atoi(argv[4]):1					/*n_threads*/
			);
		} else if(strcmp(argv[1], "decompose_bt") == 0) {
			CoreTree ct(argv[2]);						/*path*/
			ct.decompose_bp(atoi(argv[4]));
			ct.decompose_tree(atoi(argv[3]),			/*max_w*/
				argc>4?atoi(argv[4]):1					/*n_threads*/
			);
			ct.save_tmp_graph(atoi(argv[3]));

		}
		else if(strcmp(argv[1], "index-pll") == 0)
			index_pll(argv[2]);							/*path*/
		else if(strcmp(argv[1], "cross-check") == 0)
			cross_check(argv[2], 						/*path*/
				atoi(argv[3]),							/*max_w*/
				atoi(argv[4])							/*n_pairs*/
			);
		else if( strcmp(argv[1], "query-batch" ) == 0 )
			query_dis(argv[2], 							/*path*/
				atoi(argv[3]), 							/*max_w*/
				atoi(argv[4])							/*n_pairs*/
			);
		else if( strcmp(argv[1], "query-dis" ) == 0 )
			query_dis(argv[2], 							/*path*/
				atoi(argv[3]), 							/*max_w*/
				atoi(argv[4]),							/*u*/
				atoi(argv[5])							/*v*/
			);
	}

	return 0;
}
