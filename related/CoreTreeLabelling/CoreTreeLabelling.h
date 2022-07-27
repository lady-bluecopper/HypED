#ifndef CORETREELABELLING_H_
#define CORETREELABELLING_H_

#include <omp.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>
#include <utility>
#include <cstdio>
#include <cmath>
#include <cstdint>
#include <climits>
#include <iostream>
#include <fstream>
#include <functional>

using namespace std;

#define MAXN 1000000000
#define MAXINT ((unsigned) 4294967295)
#define MAXGID 100000000
#define MAXLINE 1024
#define MAXT 65000
#define MAXD 120

#define N_ROOTS 4
#define MAX_BP_THREADS 8

#define RANK_STATIC 0
#define RANK_LOCAL_STATIC 1 //LS
#define RANK_HOP_BETWEENNESS 2 //HB

typedef unsigned short tint;
typedef char dint;

vector<int> score, _score;

class IntVal{
public:
	int x;
public:
	IntVal();
	IntVal(int _x);
	bool operator<(const IntVal &v) const;
};

class DV {
public:
	DV();
	DV(int id, double val);
	int id;
	double val;
	bool operator < (const DV& v) const;
};

class TreeNode {
public:
	int id, f, h, rid, rsize, w;
	vector<int> nbr, cost; //of size <=w
	vector<int> anc; //of size h
	vector<dint> dis;
	vector<int> ch;
};

struct BPLabel {
    uint8_t bpspt_d[N_ROOTS];
    uint64_t bpspt_s[N_ROOTS][2];
};

class CoreTree {
public:
	static inline bool get_edge(char *line, int &a, int &b, int num_cnt = 2);
	static inline int get_num_cnt(string path);
	static void create_bin(string path,int rank_threads = 1,
			int rank_method = RANK_STATIC,  int rank_max_minutes = 1000000, int max_hops = 3, bool merge_equv = true);
	static void get_order(	vector<int> *con, int n, int *o, int method, int rank_threads, int rank_max_minutes, int max_hops);
	static void get_order_ls(vector<int> *con, int n, int *o, int rank_threads, int rank_max_minutes);
	static void get_order_hb(vector<int> *con, int n, int *o, int rank_threads, int rank_max_minutes, int n_hop);

public:
	string path;
	int **con, *dat, *deg, *nid, *oid, *f;
	int n_org, n, n_core;
	long long m, m_core;
	unsigned MAXDIS, MAXMOV, MASK;

public:
	CoreTree(string path);
	~CoreTree();
	void load_graph();

public:
	vector<vector<int>> nbr, cost;
	vector<int> ord, rank;
	vector<vector<pair<int,int>>> E;
	vector<TreeNode> tree;
	double t;
	vector<unsigned> *label;
	int max_w;

public:
	bool can_update(int v, int dis, char *nowdis);
	void reduce(int max_w, int n_threads);
	void create_tree();

	void compute_tree_label();
	void compute_tree_label(int x, int rsize, vector<TreeNode*> &s);
	void compute_core_label(int max_w, int n_threads);

	void decompose_bp(int n_threads);
	void decompose_tree(int max_w, int n_threads);
	void decompose_core(int max_w, int n_threads);

	void load_label_core(int max_w);
	void load_label_tree(int max_w);
	void load_label_bp();
	void load_label(int max_w);

	void save_label_core(int max_w);
	void save_label_tree(int max_w);
	void save_label_bp();

public:
	pair<int,int> **core_con;
	pair<int,int> *core_dat;
	BPLabel *core_bp;
	int *core_deg;

	void save_tmp_graph(int max_w);
	void load_tmp_graph(int max_w);

public:
	bool *usd_bp;
	BPLabel *label_bp;

public:
	void construct_bp_label(int n_threads);
	int query_by_bp(int u, int v);
	bool prune_by_bp(int u, int v, int d);

public:
	tint *last_t;
	int *dis;
	tint nowt;

public:
	void init_query();
	int query(int u, int v);
	int query_by_core(int u, int v, int d);
	int query_by_tree(int u, int v);
};

//////

bool DV::operator < (const DV& v) const {
	if( val > v.val+1e-8 ) return true;
	if( val < v.val-1e-8 ) return false;
	return id < v.id;
}

DV::DV(int id, double val) {
	this->id = id; this->val = val;
}

DV::DV() {id = -1; val=-1;}


IntVal::IntVal() {x=-1;}
IntVal::IntVal(int x) {this->x=x;}
bool IntVal::operator<(const IntVal &v) const {if(score[x] == score[v.x]) return x<v.x; return score[x] < score[v.x];}

CoreTree::CoreTree(string path) {
	deg = NULL; dat = NULL; con = NULL;
	nid = NULL; oid = NULL; f = NULL;
	n_org = 0; n = 0; m = 0; n_core = 0; m_core = 0;
	MAXDIS = 0; MAXMOV = 0; MASK = 0; max_w = 0;
	label = NULL; last_t = NULL; dis = NULL; nowt = 0;
	label_bp = NULL; usd_bp = NULL;
	core_con = NULL; core_dat = NULL;
	core_deg = NULL; core_bp = NULL;
	this->path = path; t = omp_get_wtime();
}

CoreTree::~CoreTree() {
	if(deg) delete[] deg; if(dat) delete[] dat; if(con) delete[] con;
	if(nid) delete[] nid; if(oid) delete[] oid; if(f) delete[] f;
	if(label) delete[] label; if(last_t) delete[] last_t; if(dis) delete[] dis;
	if(label_bp) delete[] label_bp; if(usd_bp) delete[] usd_bp;
	if(core_con) delete[] core_con; if(core_dat) delete[] core_dat;
	if(core_deg) delete[] core_deg; if(core_bp) delete[] core_bp;
}

void CoreTree::init_query() {
	nowt = 0;
	last_t = new tint[n_core];
	memset( last_t, 0, sizeof(tint) * n_core );
	dis = new int[n_core];
}

int CoreTree::query_by_core(int u, int v, int d) {
	++nowt;
	if(nowt == MAXT) {
		memset(last_t, 0, sizeof(tint) * n_core);
		nowt=1;
	}
	if(rank[v]==-1 && rank[u]>=0) {int x=u; u=v; v=x;}
	if(rank[v]>=0 && rank[u]>=0)
		if(tree[v].rsize < tree[u].rsize) {int x=u; u=v; v=x;}
	if(rank[v]==-1 && rank[u]==-1)
		if(label[v].size() < label[u].size()) {int x=u; u=v; v=x;}
	int mind = d;
	if(rank[u] == -1) {
		for(auto l:label[u]) {last_t[l>>MAXMOV] = nowt; dis[l>>MAXMOV] = l&MASK;}
	} else {
		TreeNode &tu = tree[u];
		TreeNode &r = tree[tu.rid];
		for(size_t i=0; i < tu.rsize; ++i) {
			int x = r.nbr[i], w = tu.dis[i];
			//if(w >= mind) continue;
			for(auto l:label[x]) {
				int y = l>>MAXMOV, nowd = (l&MASK)+w;
				if(nowd >= mind) break;
				if(last_t[y] != nowt) {last_t[y] = nowt; dis[y] = nowd;}
				else if(nowd<dis[y]) dis[y] = nowd;
			}
		}
	}
	if(rank[v] == -1) {
		for(auto l:label[v]) if(last_t[l>>MAXMOV] == nowt) mind = min(mind, (int)(l&MASK)+dis[l>>MAXMOV]);
	} else {
		TreeNode &tv = tree[v];
		TreeNode &r = tree[tv.rid];
		for(size_t i = 0; i < tv.rsize; ++i ) {
			int x = r.nbr[i], w = tv.dis[i];
			//if(w >= mind) continue;
			for(auto l:label[x]) {
				int nowd = (l&MASK)+w;
				if(nowd >= mind) break;
				if(last_t[l>>MAXMOV] == nowt) mind = min(mind, nowd+dis[l>>MAXMOV]);
			}
		}
	}
	return mind;
}

int CoreTree::query_by_tree(int u, int v) {
	TreeNode &tu = tree[u], &tv = tree[v];
	int len = min(tu.h,tv.h);
	int d = INT_MAX;
	for(int i = tu.rsize, j = 0; i < len && tu.anc[j] == tv.anc[j]; ++i,++j)
		d = min(d, tu.dis[i]+tv.dis[i]);
	return d;
}

int CoreTree::query(int u, int v) {
	if( u == v ) return 0;
	int type = 0;
	int oldu = u;
	int oldv = v;
	u = nid[u]; v = nid[v];
	if(u < 0) {u = -u-1; type = 1;} else if(u >= MAXN) {u = u-MAXN; type = 2;}
	if(v < 0) {v = -v-1; type = 1;} else if(v >= MAXN) {v = v-MAXN; type = 2;}
	if(u == v) return type == 1 ? (deg[u] == 0 ? INT_MAX : 2) : 1;
	if( u >= n || v >= n ) return INT_MAX;

	int d = query_by_bp(u,v);
	int firstd = d;
	d = query_by_core(u,v,d);
	printf( "u=%d, remapu=%d, v=%d, remapv=%d, bp=%d, core=%d\n", oldu, u, oldv, v, firstd, d );
	if(rank[u] >= 0 && rank[v] >= 0 && tree[u].rid == tree[v].rid)
		d = min(d, query_by_tree(u, v));
	return d;
}

void CoreTree::save_tmp_graph(int max_w) {
	printf( "Saving Tmp Graph...\n" );

	char stw[16];
	sprintf(stw, "%d", max_w);

	FILE *fout = fopen( (path+"tmp-" + string(stw) + ".bin").c_str(), "wb" );

	fwrite(&n, sizeof(int), 1, fout);
	fwrite(&m_core, sizeof(long long), 1, fout);
	fwrite(rank.data(), sizeof(int), n, fout);
	fwrite(usd_bp, sizeof(bool), n, fout);
	vector<int> cored(n,0);
	for(int i = 0; i < n; ++i)
		if(rank[i] == -1) cored[i] = (int) E[i].size();
	fwrite(cored.data(), sizeof(int), n, fout);
	for(int i = 0; i < n; ++i)
		if(rank[i] == -1)
			fwrite(E[i].data(), sizeof(pair<int,int>), E[i].size(), fout);
	core_bp = new BPLabel[n_core];
	int p = 0;
	for(int i = 0; i < n; ++i)
		if(rank[i] == -1 && !usd_bp[i])
			core_bp[p++] = label_bp[i];
	printf( "n_bc=%d\n", p );
	fwrite(core_bp, sizeof(BPLabel), p, fout);
	fclose(fout);
	printf( "Tmp Graph Saved!\n" );
}

void CoreTree::load_tmp_graph(int max_w) {
	printf( "Loading Tmp Graph...\n" );

	char stw[16];
	sprintf(stw, "%d", max_w);

	FILE *fin = fopen( (path+"tmp-" + string(stw) + ".bin").c_str(), "rb" );
	fread(&n, sizeof(int), 1, fin);
	n_core = 0;
	fread(&m_core, sizeof(long long), 1, fin);
	rank.resize(n); usd_bp = new bool[n];
	fread(rank.data(), sizeof(int), n, fin);
	fread(usd_bp, sizeof(bool), n, fin);

	int n_bc = 0;
	for(int i = 0; i < n; ++i) if(rank[i] == -1) {++n_core; if(!usd_bp[i]) ++n_bc;}
	printf( "n_bc=%d\n", n_bc );
	core_deg = new int[n]; core_dat = new pair<int,int>[m_core]; core_con = new pair<int,int>*[n];
	fread(core_deg, sizeof(int), n, fin);
	fread(core_dat, sizeof(pair<int,int>), m_core, fin);
	long long p = 0;
	for(int i = 0; i < n; ++i)
		if(rank[i] ==-1) {
			core_con[i] = core_dat + p;
			p += core_deg[i];
		}
	printf( "m_core=%lld\n", p );
	core_bp = new BPLabel[n_bc];
	fread(core_bp, sizeof(BPLabel), n_bc, fin);
	fclose(fin);
	printf( "Tmp Graph Loaded!\n" );
}

void CoreTree::save_label_tree(int max_w) {
	printf( "Saving Tree Label...\n" );
	char stw[16];
	sprintf(stw, "%d", max_w);

	FILE *fout = fopen( (path+"label-tree-" + string(stw) + ".bin").c_str(), "wb" );
	fwrite(&n, sizeof(int), 1, fout);
	fwrite(rank.data(), sizeof(int), n, fout);
	int count = n + 1;
	for(int i = 0; i < n; ++i)
		if(rank[i] >= 0) {
			TreeNode &tn = tree[i];
			fwrite(&tn.rid, sizeof(int), 1, fout);
			fwrite(&tn.rsize, sizeof(int), 1, fout);
			fwrite(&tn.h, sizeof(int), 1, fout);
			fwrite(&tn.w, sizeof(int), 1, fout);
			fwrite(tn.nbr.data(), sizeof(int), tn.w, fout);
			fwrite(tn.anc.data(), sizeof(int), tn.h-tn.w, fout);
			fwrite(tn.dis.data(), sizeof(dint), tn.h, fout);
			count += 4;
			count += tn.h;
			count += tn.h;
		}
	fclose(fout);
	printf( "Tree Label Saved With %d Elements.\n", count );
}

void CoreTree::save_label_bp() {
	printf( "Saving BP Label...\n" );
	FILE *fout = fopen( (path+"label-bp.bin").c_str(), "wb" );

	fwrite(&n, sizeof(int), 1, fout);
	fwrite(usd_bp, sizeof(bool), n, fout);
	fwrite(label_bp, sizeof(BPLabel), n, fout);
	fclose(fout);
        int count = 1 + n + n * sizeof(BPLabel);

	printf( "BP Label Saved With %d Elements.\n", count );
}

void CoreTree::save_label_core(int max_w) {
	printf( "Saving Core Label...\n" );
	char stw[16];
	sprintf(stw, "%d", max_w);
	FILE *fout = fopen( (path+"label-core-" + string(stw) + ".bin").c_str(), "wb" );

	fwrite(&n, sizeof(int), 1, fout);
	for(int i = 0; i < n; ++i) {
		int len = (int) label[i].size();
		fwrite(&len, sizeof(int), 1, fout);
	}
	int count = n + 2;
	for(int i = 0; i < n; ++i)
		if(label[i].size() > 0) {
			fwrite(label[i].data(), sizeof(unsigned), label[i].size(), fout);
			count += label[i].size();
		}
	fwrite(&MAXMOV, sizeof(int), 1, fout);
	fclose(fout);
	printf( "Core Label Saved With %d Elements.\n", count );
}

void CoreTree::load_label_tree(int max_w) {
	char stw[16];
	sprintf(stw, "%d", max_w);

	ifstream treeFile;
        treeFile.open((path+"label-tree-" + string(stw) + ".bin").c_str());
        if(!treeFile) {
		return;
	}

	try {
		FILE *fin = fopen( (path+"label-tree-" + string(stw) + ".bin").c_str(), "rb" );
		fread(&n, sizeof(int), 1, fin);
		rank.resize(n);

		fread(rank.data(), sizeof(int), n, fin);
		n_core = 0;
		for(int i = 0; i < n; ++i) if(rank[i]==-1) ++n_core;
		tree.resize(n);

		for(int i = 0; i < n; ++i)
			if(rank[i] >= 0) {
				TreeNode &tn = tree[i];
				fread(&tn.rid, sizeof(int), 1, fin);
				fread(&tn.rsize, sizeof(int), 1, fin);
				fread(&tn.h, sizeof(int), 1, fin);
				fread(&tn.w, sizeof(int), 1, fin);
				tn.nbr.resize(tn.w); tn.anc.resize(tn.h); tn.dis.resize(tn.h);
				fread(tn.nbr.data(), sizeof(int), tn.w, fin);
				fread(tn.anc.data(), sizeof(int), tn.h-tn.w, fin);
				fread(tn.dis.data(), sizeof(dint), tn.h, fin);
			}
		fclose(fin);
	} catch(...) {
		// printf("Caught exception in reading tree-index file.\n");
	}
}

void CoreTree::load_label_core(int max_w) {
	char stw[16];
	sprintf(stw, "%d", max_w);

	ifstream coreFile;
	coreFile.open((path+"label-core-" + string(stw) + ".bin").c_str());
	if(coreFile) {
		try {
			FILE *fin = fopen( (path+"label-core-" + string(stw) + ".bin").c_str(), "rb" );
			label = new vector<unsigned>[n];

			fread(&n, sizeof(int), 1, fin);
			int *len = new int[n];
			fread(len, sizeof(int), n, fin);

			for( int i = 0; i < n; ++i ) {
				label[i].resize(len[i]);
				fread(label[i].data(), sizeof(unsigned), len[i], fin);
			}
			delete[] len;

			fread(&MAXMOV, sizeof(unsigned), 1, fin);
			MAXDIS = 1<<MAXMOV; MASK = MAXDIS-1;
			fclose(fin);
		} catch(...) {
			// printf("Caught exception in reading core-index file.\n");
		}
	}
}

void CoreTree::load_label_bp() {
	FILE *fin = fopen( (path+"label-bp.bin").c_str(), "rb" );
	usd_bp = new bool[n];
	label_bp = new BPLabel[n];

	try {
		fread(&n, sizeof(int), 1, fin);
		fread(usd_bp, sizeof(bool), n, fin);
		fread(label_bp, sizeof(BPLabel), n, fin);
		fclose(fin);
	} catch(...) {
		// printf("Caught exception in reading bp-index.\n");
	}
}

void CoreTree::load_label(int max_w) {
	this->max_w = max_w;
	load_label_tree(max_w);
	load_label_bp();
	load_label_core(max_w);
	load_graph();
	init_query();
}

void CoreTree::decompose_bp(int n_threads) {
	if(con == NULL) load_graph();
	construct_bp_label(n_threads);
	save_label_bp();
}

void CoreTree::decompose_tree(int max_w, int n_threads) {
	if(con == NULL) load_graph();
	reduce(max_w, n_threads);
	create_tree();
	compute_tree_label();
	save_label_tree(max_w);
}

void CoreTree::decompose_core(int max_w, int n_threads) {
	load_tmp_graph(max_w);
	compute_core_label(max_w, n_threads);
}

bool CoreTree::can_update(int v, int dis, char *nowdis) {
	for(auto l:label[v]) {
		int d = l&MASK, u = l>>MAXMOV;
		//if(d>dis) return true;
		if( nowdis[u] >= 0 && nowdis[u] + d <= dis ) return false;
	}
	return true;
}

void CoreTree::compute_core_label(int max_w, int n_threads) {
	printf( "Computing Core Label...\n" );

	omp_set_num_threads(n_threads);
	if(n_core == 0) {printf("No core nodes!\n"); return;}
	MAXDIS = 2; MAXMOV = 1;
	while( MAXINT / (n_core * 2) >= MAXDIS ) {MAXDIS *= 2;++MAXMOV;}
	MASK = MAXDIS - 1;
	printf( "MAXDIS=%d\n", MAXDIS );

	vector<int> *pos = new vector<int> [n];
	label = new vector<unsigned>[n];

	int p = 0;
	vector<int> vid;
	vector<int> cid(n);

	for(int u = 0; u < n; ++u)
		if(rank[u]==-1 && !usd_bp[u]) {
			vid.push_back(u);
			cid[u] = p;
			label[u].push_back(((p++)<<MAXMOV) | 0);
			pos[u].push_back(1);
		} else if(rank[u] == -1) pos[u].push_back(0);

	printf( "n_bc=%d,n_core=%d,n=%d\n", p, n_core, n );
	int dis = 1;

	for( long long cnt = 1; cnt && dis <= (int) MAXDIS; ++dis ) {
		cnt = 0;
		vector<unsigned> *label_new = new vector<unsigned>[n];
		#pragma omp parallel
		{
			int pid = omp_get_thread_num(), np = omp_get_num_threads();
			long long local_cnt = 0;

			vector<char> used(n_core, 0);
			vector<int> cand;
			char *nowdis = new char[n_core];
			memset( nowdis, -1, sizeof(char) * n_core );

			#pragma omp for schedule(dynamic)
			for( int u = 0; u < n; ++u ) {
				if(rank[u] >= 0 || usd_bp[u]) continue;
				for(auto &l:label[u]) nowdis[l>>MAXMOV] = l&MASK;
				cand.clear();
				//for(auto &e:E[u]) {
				for(int i = 0; i < core_deg[u]; ++i) {
					//int x = e.first, w = e.second;
					int x = core_con[u][i].first, w = core_con[u][i].second;
					if(w>dis) continue;
					for( int j = w==dis?0:pos[x][dis-w-1]; j < pos[x][dis-w]; ++j ) {
						int v = label[x][j]>>MAXMOV;
						if( vid[v] >= u ) break;
						if(!used[v]) {
							used[v] = (!prune_by_bp(cid[u], v, dis)) && can_update(vid[v], dis, nowdis) ? 1 : -1;
							cand.push_back(v);
						}
					}
				}

				int n_cand = 0;
				for( int i = 0; i < (int) cand.size(); ++i ) {
					if(used[cand[i]] == 1) cand[n_cand++] = cand[i];
					used[cand[i]] = 0;
 				}

				cand.resize(n_cand); sort(cand.begin(), cand.end());

				size_t p = 0;
				for(auto v:cand) {
					if(label_new[u].size() > 100 && label_new[u].size() == label_new[u].capacity()) label_new[u].reserve(label_new[u].capacity() * 1.2);
					label_new[u].push_back((((unsigned)v)<<MAXMOV) | (unsigned) dis);
					++local_cnt;
				}
				for( int i = 0; i < (int) label[u].size(); ++i ) nowdis[label[u][i]>>MAXMOV] = -1;
			}

			if(pid==0) printf( "num_thread=%d,", np );
			#pragma omp critical
			{
				cnt += local_cnt;
			}
			delete[] nowdis;
		}

		#pragma omp parallel
		{
			#pragma omp for schedule(dynamic)
			for( int u = 0; u < n; ++u ) {
				if(rank[u] >= 0) continue;
				label[u].insert(label[u].end(), label_new[u].begin(), label_new[u].end());
				//vector<unsigned>(label[u]).swap(label[u]);
				vector<unsigned>().swap(label_new[u]);
				pos[u].push_back((int)label[u].size());
			}
		}

		delete[] label_new;
		printf( "dis=%d,cnt=%lld,t=%0.3lf secs\n", dis, cnt,  omp_get_wtime()-t );
	}

	double tt = 0, max_label = 0;
	for( int i = 0; i < n; ++i )
		if(rank[i] == -1) {
			tt += label[i].size() * 4;
			max_label = max(max_label, label[i].size()*1.0);
		}

	printf( "Core label size=%0.3lfMB, Max core label size=%0.0lf, Avg core label size=%0.3lf, Time = %0.3lf sec\n",
			tt/(1024*1024.0), max_label, tt*0.25/n, omp_get_wtime()-t );
	// save core-index
	save_label_core(max_w);

	delete[] pos;
}


void CoreTree::compute_tree_label(int x, int rsize, vector<TreeNode*> &s) {
	s.push_back(&tree[x]);
	TreeNode &tn = tree[x];
	tn.dis.resize(tn.h);
	int pos = 0;
	vector<int> p(tn.w);
	for(int j = 0; j < tn.w; ++j) {
		while(s[pos]->id != tn.nbr[j]) ++pos;
		p[j] = pos;
	}

	for(int i = 0; i < tn.h-1; ++i) {
		tn.dis[i] = -1;
		for(int j = 0; j < tn.w; ++j) {
			int w = tn.cost[j], k = p[j], nowdis = -1;
			if(k<=i) {if(i>=rsize) nowdis = s[i]->dis[k]; else if(k==i) nowdis=0;}
			else if(k>=rsize) nowdis = s[k]->dis[i];
			if(nowdis>=0 && (tn.dis[i]==-1 || nowdis+w<tn.dis[i])) tn.dis[i]=min(nowdis+w,MAXD);
		}
	}
	tn.dis[tn.h-1] = 0;
	for(int &u:tree[x].ch)
		compute_tree_label(u, rsize, s);
	s.pop_back();
}

void CoreTree::compute_tree_label() {
	printf( "Computing Tree Label...\n" );
	vector<TreeNode*> s;
	for(int v=0; v<n; ++v)
		if(rank[v] >= 0 && tree[v].f == -1) {
			s.clear();
			for(int i=0; i<tree[v].w; ++i) s.push_back(&tree[tree[v].nbr[i]]);
			compute_tree_label(v, tree[v].rsize, s);
		}
	double t_size = 0;
	int maxdis = 0;
	for(int v=0; v<n; ++v) {
		if(rank[v] >= 0) {
			t_size += tree[v].dis.size() * 1.0 * (sizeof(int)+sizeof(dint));
			vector<pair<int,int>>().swap(E[v]);
			for(auto &d:tree[v].dis) maxdis = max(maxdis, (int)d);
		} else vector<pair<int,int>>(E[v]).swap(E[v]);
	}

	printf( "Tree Label Computed, t=%0.3lf secs, maxdis=%d, tree label size=%0.3lf MB\n", omp_get_wtime()-t, maxdis, t_size/(1024.0*1024.0));
}


void CoreTree::create_tree() {
	printf( "Creating Tree...\n" );
	tree.resize(n);
	for(int u = 0; u < n; ++u) tree[u].id = u;
	vector<pair<int,int>> v_pair;
	int maxh = 0, cnt_root = 0, maxdep = 0, max_sub_tree = 1;
	vector<int> tcnt(n,0);
	double tw = 0;
	for(int i = (int) ord.size()-1; i >= 0; --i) {
		int x = ord[i];
		TreeNode &tn = tree[x];
		v_pair.clear();
		for(int j = 0; j < (int) nbr[x].size(); ++j) {
			int y = nbr[x][j];
			if(rank[y] == -1) v_pair.push_back(make_pair(n,j));
			else v_pair.push_back(make_pair(rank[y],j));
		}
		sort(v_pair.begin(),v_pair.end());
		reverse(v_pair.begin(), v_pair.end());
		int w = (int) nbr[x].size();
		tn.nbr.resize(w);
		tn.cost.resize(w);
		for(int j=0; j<w; ++j) {
			tn.nbr[j] = nbr[x][v_pair[j].second];
			tn.cost[j] = cost[x][v_pair[j].second];
		}

		tn.w = w;
		tn.id = x;
		tn.f = -1;
		for(auto &u:nbr[x])
			if(rank[u]!=-1 && (tn.f==-1 || rank[u] < rank[tn.f]))
				tn.f = u;
		if(tn.f == -1) {
			tn.h = tn.w + 1;
			++cnt_root;
			++tcnt[x];
			tn.rid = x;
			tn.rsize = tn.w;
			tn.anc.push_back(x);
		} else {
			tn.h = tree[tn.f].h+1;
			tree[tn.f].ch.push_back(x);
			tn.rid = tree[tn.f].rid;
			++tcnt[tn.rid];
			max_sub_tree = max(max_sub_tree, tcnt[tn.rid]);
			tn.rsize = tree[tn.f].rsize;
			tn.anc = tree[tn.f].anc;
			tn.anc.push_back(x);
		}
		tw += tn.rsize;
		maxh = max(maxh, tn.h);
		maxdep = max(maxdep, (int)tn.anc.size());
	}
	printf( "Core tree constructed, maxh=%d, maxdep=%d, cnt_root=%d, max_stree=%d, avg_rsize=%0.3lf, t=%0.3lf secs\n",
			maxh, maxdep, cnt_root, max_sub_tree, tw/(n-n_core), omp_get_wtime()-t);
}

void CoreTree::reduce(int max_w, int n_threads) {
	omp_set_num_threads(n_threads);
	this->max_w = max_w;
	score.resize(n); _score.resize(n);
	vector<bool> changed(n,false);
	set<IntVal> q;
	nbr.resize(n); cost.resize(n);
	rank.resize(n); fill(rank.begin(), rank.end(), -1);
	int r = 0;

	for(int i = 0; i < n; ++i) score[i] = _score[i] = deg[i];

	printf( "Initializing q..." );
	vector<bool> active(n,false);
	for(int u = 0; u < n; ++u)
		if(deg[u]<max_w) {q.insert(IntVal(u)); active[u] = true;}

	printf( ", t=%0.3lf secs\nInitializing E...", omp_get_wtime()-t );
	E.resize(n);

	for(int u = 0; u < n; ++u)
		for(int i = 0; i < deg[u]; ++i) E[u].push_back(make_pair(con[u][i],1));
	printf( ", t=%0.3lf secs\nReducing Graph...\n", omp_get_wtime()-t );

	int cnt = 0;
	vector<pair<int,int>> tmp;
	while(!q.empty()) {
		int x = q.begin()->x;
		while(changed[x]) {
			q.erase(x);
			score[x] = _score[x];
			q.insert(x);
			changed[x] = false;
			x = q.begin()->x;
		}
		if(score[x] >= max_w) break;
		ord.push_back(x);
		q.erase(x);
		rank[x] = r++;
		for(auto &it:E[x]){nbr[x].push_back(it.first); cost[x].push_back(it.second);}

		for(auto &y:nbr[x]) {
			if(E[y].size() >= max_w * 2) {active[y] = false; q.erase(y);}
			if(!active[y]) continue;
			for(size_t i=0;i<E[y].size();++i)
				if(E[y][i].first == x) {E[y].erase(E[y].begin()+i); break;}
			_score[y] = (int) E[y].size();
			changed[y] = true;
		}

		for(size_t i = 0; i < nbr[x].size(); ++i) {
			int u = nbr[x][i];

			if(!active[u]) {
				E[u].push_back(make_pair(x,-cost[x][i]));
				continue;
			}
			tmp.clear();
			size_t j=0, k=0;
			while(j<nbr[x].size()&&k<E[u].size())
				if(j==i) ++j;
				else if(nbr[x][j]<E[u][k].first) {tmp.push_back(make_pair(nbr[x][j], cost[x][i]+cost[x][j])); ++j;}
				else if(nbr[x][j]>E[u][k].first) {tmp.push_back(E[u][k]); ++k;}
				else {
					if(E[u][k].second < cost[x][i]+cost[x][j]) tmp.push_back(E[u][k]);
					else tmp.push_back(make_pair(nbr[x][j], cost[x][i]+cost[x][j]));
					++j; ++k;
				}
			for(;j<nbr[x].size();++j) if(j!=i) tmp.push_back(make_pair(nbr[x][j], cost[x][i]+cost[x][j]));
			for(;k<E[u].size();++k) tmp.push_back(E[u][k]);
			E[u] = tmp;
			if(_score[u] != (int) E[u].size()) {
				changed[u] = true;
				_score[u] = (int) E[u].size();
			}
		}

		if((++cnt) * score[x] > 1000000) {
			printf( "%d nodes reduced, score[x]=%d, remaining size=%0.3lf%% t=%0.3lf secs\n",
					r, (n-r)*100.0/n, score[x], omp_get_wtime()-t);
			cnt = 0;
		}
	}

	printf( "Reordering edges...\n" );

	#pragma omp parallel
	{
		vector<int> ve;
		vector<int> buf(n,-1);

		#pragma omp for schedule(dynamic)
		for(int u = 0; u < n; ++u)
			if(!active[u] && E[u].size()>0) {
				auto &e = E[u];
				ve.clear();
				for(size_t i = 0; i < e.size(); ++i) {
					if(e[i].second >= 0) {
						int v = e[i].first, w = e[i].second;
						if(rank[v]>=0) continue;
						else if(buf[v] == -1) {buf[v] = w; ve.push_back(v);}
						else if(w < buf[v]) buf[v] = w;
					} else {
						auto &s = E[e[i].first];
						for(size_t j=0; j<s.size(); ++j) {
							int v = s[j].first, w = s[j].second - e[i].second;
							if(v == u || rank[v]>=0) continue;
							else if(buf[v] == -1) {buf[v] = w; ve.push_back(v);}
							else if(w < buf[v]) buf[v] = w;
						}
					}
				}
				e.resize(ve.size());
				for(size_t i = 0; i < ve.size(); ++i) {
					e[i]=make_pair(ve[i], buf[ve[i]]);
					buf[ve[i]] = -1;
				}
				sort(e.begin(), e.end());
			}
	}

	n_core = 0;
	m_core = 0;
	for(int u = 0; u < n; ++u)
		if( rank[u] == -1 ) {
			++n_core;
			m_core += (int) E[u].size();
		}

	printf( "Reducing finished, t=%0.3lf secs\nn_core=%d,m_core=%lld,node_rate=%0.3lf,edge_rate=%0.3lf\n",
			omp_get_wtime()-t, n_core, m_core, n_core*1.0/n, m_core*1.0/m );
}


void CoreTree::load_graph() {
	long long p = 0;
	FILE* fin = fopen( (path+"graph-dis.bin").c_str(), "rb" );
	fread( &n, sizeof(int), 1, fin );
	fread( &m, sizeof(long long), 1, fin );
	deg = new int[n]; dat = new int[m]; con = new int*[n]; nid = new int[n];
	fread( deg, sizeof(int), n, fin );
	fread( dat, sizeof(int), m, fin );
	fread( nid, sizeof(int), n, fin );
	fclose(fin);
	for( int i = 0; i < n; ++i ) {con[i] = dat+p; p+= deg[i];}
	int nown = n-1; while(nown>=0 && deg[nown] == 0) --nown;
	nown += 1; if( nown < 2 ) nown = 2;
	n_org = n; n = nown;
	// printf( "graph=%s, n_org=%d, n=%d, m=%lld\n", path.c_str(), n_org, n, m);
}

void CoreTree::construct_bp_label(int n_threads) {
	printf( "Constructing BP Label...\n" );
	label_bp = new BPLabel[n];
	usd_bp = new bool[n];
	memset( usd_bp, 0, sizeof(bool) * n );
	vector<int> v_vs[N_ROOTS];

	int r = 0;
	for (int i_bpspt = 0; i_bpspt < N_ROOTS; ++i_bpspt) {
		while (r < n && usd_bp[r]) ++r;
		if (r == n) {
			for (int v = 0; v < n; ++v) label_bp[v].bpspt_d[i_bpspt] = MAXD;
			continue;
		}
		usd_bp[r] = true;
		v_vs[i_bpspt].push_back(r);
		int ns = 0;
		for (int i = 0; i < deg[r]; ++i) {
			int v = con[r][i];
			if (!usd_bp[v]) {
				usd_bp[v] = true;
				v_vs[i_bpspt].push_back(v);
				if (++ns == 64) break;
			}
		}
	}

	omp_set_num_threads(min(min(n_threads, N_ROOTS),MAX_BP_THREADS));
	#pragma omp parallel
	{
		int pid = omp_get_thread_num(), np = omp_get_num_threads();
		if( pid == 0 ) printf( "n_threads_bp = %d\n", np );
		vector<uint8_t> tmp_d(n);
		vector<pair<uint64_t, uint64_t> > tmp_s(n);
		vector<int> que(n);
		vector<pair<int, int> > child_es(m/2);

		#pragma omp for schedule(dynamic)
		for (int i_bpspt = 0; i_bpspt < N_ROOTS; ++i_bpspt) {
			printf( "[%d]", i_bpspt );

			if( v_vs[i_bpspt].size() == 0 ) continue;
			fill(tmp_d.begin(), tmp_d.end(), MAXD);
			fill(tmp_s.begin(), tmp_s.end(), make_pair(0, 0));

			r = v_vs[i_bpspt][0];
			int que_t0 = 0, que_t1 = 0, que_h = 0;
			que[que_h++] = r;
			tmp_d[r] = 0;
			que_t1 = que_h;

			for( size_t i = 1; i < v_vs[i_bpspt].size(); ++i) {
				int v = v_vs[i_bpspt][i];
				que[que_h++] = v;
				tmp_d[v] = 1;
				tmp_s[v].first = 1ULL << (i-1);
			}

			for (int d = 0; que_t0 < que_h; ++d) {
				int num_child_es = 0;

				for (int que_i = que_t0; que_i < que_t1; ++que_i) {
					int v = que[que_i];

					for (int i = 0; i < deg[v]; ++i) {
						int tv = con[v][i];
						int td = d + 1;

						if (d == tmp_d[tv]) {
							if (v < tv) {
								tmp_s[v].second |= tmp_s[tv].first;
								tmp_s[tv].second |= tmp_s[v].first;
							}
						} else if( d < tmp_d[tv]) {
							if (tmp_d[tv] == MAXD) {
								que[que_h++] = tv;
								tmp_d[tv] = td;
							}
							child_es[num_child_es].first  = v;
							child_es[num_child_es].second = tv;
							++num_child_es;
						}
					}
				}

				for (int i = 0; i < num_child_es; ++i) {
					int v = child_es[i].first, c = child_es[i].second;
					tmp_s[c].first  |= tmp_s[v].first;
					tmp_s[c].second |= tmp_s[v].second;
				}

				que_t0 = que_t1;
				que_t1 = que_h;
			}

			for (int v = 0; v < n; ++v) {
				label_bp[v].bpspt_d[i_bpspt] = tmp_d[v];
				label_bp[v].bpspt_s[i_bpspt][0] = tmp_s[v].first;
				label_bp[v].bpspt_s[i_bpspt][1] = tmp_s[v].second & ~tmp_s[v].first;
			}
		}
	}
	printf( "\nBP Label Constructed, bp_size=%0.3lf MB, t = %0.3lf secs\n", sizeof(BPLabel)*n/(1024.0*1024.0), omp_get_wtime() - t );
}

int CoreTree::query_by_bp(int u, int v) {
	BPLabel &idx_u = label_bp[u], &idx_v = label_bp[v];
	int d = MAXD;
	for (int i = 0; i < N_ROOTS; ++i) {
		int td = idx_u.bpspt_d[i] + (int) idx_v.bpspt_d[i];
		if (td - 2 <= d)
			td += (idx_u.bpspt_s[i][0] & idx_v.bpspt_s[i][0]) ? -2 :
					((idx_u.bpspt_s[i][0] & idx_v.bpspt_s[i][1]) | (idx_u.bpspt_s[i][1] & idx_v.bpspt_s[i][0])) ? -1 : 0;
		if (td < d) d = td;
	}
	return d == MAXD ? INT_MAX : d;
}

bool CoreTree::prune_by_bp(int u, int v, int d) {
	BPLabel &idx_u = core_bp[u], &idx_v = core_bp[v];
	for (int i = 0; i < N_ROOTS; ++i) {
		int td = idx_u.bpspt_d[i] + idx_v.bpspt_d[i];
		if (td - 2 <= d)
			td += (idx_u.bpspt_s[i][0] & idx_v.bpspt_s[i][0]) ? -2 :
					((idx_u.bpspt_s[i][0] & idx_v.bpspt_s[i][1]) | (idx_u.bpspt_s[i][1] & idx_v.bpspt_s[i][0])) ? -1 : 0;
		if (td <= d) return true;
	}
	return false;
}

bool CoreTree::get_edge(char *line, int &a, int &b, int num_cnt) {
	if( !isdigit(line[0]) ) return false;
	vector<char*> v_num;
	int len = (int) strlen(line);
	for( int i = 0; i < len; ++i )
		if( !isdigit(line[i]) && line[i] != '.') line[i] = '\0';
		else if(i == 0 || !line[i-1]) v_num.push_back(line+i);
	if( (int) v_num.size() != num_cnt ) return false;
	sscanf( v_num[0], "%d", &a );
	sscanf( v_num[1], "%d", &b );
	return true;
}

int CoreTree::get_num_cnt(string path) {
	FILE *fin = fopen( (path + "graph.txt").c_str(), "r" );
	char line[MAXLINE];
	int cnt = 0, min_cnt = 100;

	while( fgets( line, MAXLINE, fin ) && cnt < 10 ) {
		if( !isdigit(line[0]) ) continue;
		vector<char*> v_num;
		int len = (int) strlen(line);
		for( int i = 0; i < len; ++i )
			if( !isdigit(line[i]) && line[i] != '.' ) line[i] = '\0';
			else if(i == 0 || !line[i-1]) v_num.push_back(line+i);
		if( (int) v_num.size() < 2 ) continue;
		min_cnt = min(min_cnt, (int) v_num.size());
		++cnt;
	}
	fclose( fin );
	return min_cnt;
}

void CoreTree::get_order(	vector<int> *con, int n, int *o, int method, int rank_threads, int rank_max_minutes, int max_hops) {
	printf( "method=%d\n", method );
	if( method == RANK_STATIC ) {
		printf( "Ranking Method = RANK_STATIC\n" );
		DV *f = new DV[n];
		for( int i = 0; i < n; ++i )
			f[i].id = i, f[i].val = con[i].size() * 1.0;
		sort(f, f + n);
		for(int i = 0; i < n; ++i) o[i] = f[i].id;
		delete[] f;
	} else if( method == RANK_LOCAL_STATIC ) {
		//get_order_ls(con, n, o, rank_threads, rank_max_minutes);
	} else if( method == RANK_HOP_BETWEENNESS ) {
		//get_order_hb(con, n, o, rank_threads, rank_max_minutes, max_hops);
	}
}

void CoreTree::get_order_ls(vector<int> *con, int n, int *o,  int rank_threads, int rank_max_minutes) {
	printf( "Ranking Method = RANK_LOCAL_STATIC\n" );

}

void CoreTree::get_order_hb(vector<int> *con, int n, int *o, int rank_threads, int rank_max_minutes, int n_hop) {
	printf( "Ranking Method = RANK_HOP_BETWEENNESS\n" );

}

void CoreTree::create_bin(string path, int rank_threads ,int rank_method,  int rank_max_minutes, int max_hops, bool merge_equv ) {
	FILE *fin = fopen( (path + "graph.txt").c_str(), "r" );
	char line[MAXLINE];
	int n = 0, a, b, num_cnt = get_num_cnt(path);
	vector< pair<int,int> > el;
	long long cnt = 0, m = 0;

	printf( "Loading text, num_cnt=%d...\n", num_cnt );
	while( fgets( line, MAXLINE, fin ) ) {
		if( !get_edge(line, a, b, num_cnt) ) continue;
		if( a < 0 || b < 0 || a == b ) continue;
		el.push_back(make_pair(a, b));
		// assumes node ids are 0 .... n-1
		n = max(max(n, a+1), b+1);
		if( (++cnt) % (long long) 10000000 == 0 ) printf( "%lld lines finished\n", cnt );
	}
	printf( "lines read %lld , edges saved %d \n", cnt, el.size() );
	fclose( fin );

	double nowt = omp_get_wtime();
	// map v -> list of adjacent nodes
	vector<int> *con = new vector<int>[n];
	printf( "Deduplicating...\n" );

	for(size_t i = 0; i < el.size(); ++i) {
		con[el[i].first].push_back(el[i].second);
		con[el[i].second].push_back(el[i].first);
	}
    // sort and remove duplicate neighbours for each node
	for( int i = 0; i < n; ++i )
		if( con[i].size() > 0 ){
			// sort adjacent nodes of node i
			sort( con[i].begin(), con[i].end() );
			int p = 1;
			for( int j = 1; j < (int) con[i].size(); ++j )
				if( con[i][j-1] != con[i][j] ) con[i][p++] = con[i][j];
			con[i].resize( p ); m += p;
		}

    printf( "Distinct adjacent pairs %lld \n", m );
	long long *f1 = new long long[n];
	memset( f1, 0, sizeof(long long) * n );

	long long *f2 = new long long[n];
	memset( f2, 0, sizeof(long long) * n );

	if( !merge_equv ) {
		for( int i = 0; i < n; ++i ) f1[i] = i, f2[i] = i;
	} else {
		// some edges are removed and degrees may change
		printf( "Merging...\n" );
		long long s = 0;
		long long *nows = new long long[m+n+1];
		int *nowt = new int[m+n+1];
		memset( nowt, 0, sizeof(int) * (m+n+1) );

		for( int v = 0; v < n; ++v )
			for( int i = 0; i < (int) con[v].size(); ++i ) {
				int u = con[v][i];
				if( nowt[f1[u]] != (v+1) ) {
					++s;
					nows[f1[u]] = s;
					nowt[f1[u]] = (v+1);
					f1[u] = s;
				} else f1[u] = nows[f1[u]];
			}

		for( int v = 0; v < n; ++v )
			if( nowt[f1[v]] != -1 ) {
				nows[f1[v]] = v;
				nowt[f1[v]] = -1;
				f1[v] = v;
			} else f1[v] = nows[f1[v]];

		s = 0;
		memset( nowt, 0, sizeof(int) * (m+n+1) );
		for( int v = 0; v < n; ++v )
			for( int i = 0; i <= (int) con[v].size(); ++i ) {
				int u = (i == (int) con[v].size()) ? v : con[v][i];
				if( nowt[f2[u]] != (v+1) ) {
					++s;
					nows[f2[u]] = s;
					nowt[f2[u]] = (v+1);
					f2[u] = s;
				} else f2[u] = nows[f2[u]];
			}

		for( int v = 0; v < n; ++v )
			if( nowt[f2[v]] != -1 ) {
				nows[f2[v]] = v;
				nowt[f2[v]] = -1;
				f2[v] = v;
			} else f2[v] = nows[f2[v]];

		delete[] nows; delete[] nowt;

		long long cnt1_n = 0, cnt1_m = 0, cnt2_n = 0, cnt2_m = 0;
		for( int i = 0; i < n; ++i ) {
			if( f1[i] != i ) {
				++cnt1_n;
				cnt1_m += (int) con[i].size();
			}
			if( f2[i] != i ) {
				++cnt2_n;
				cnt2_m += (int) con[i].size();
			}
		}

		m = 0;
		for( int i = 0; i < n; ++i ) {
			if( f1[i] != i || f2[i] != i ) {con[i].clear(); continue;}
			int p = 0;
			for( int j = 0; j < (int) con[i].size(); ++j ) {
				int v = con[i][j];
				if( f1[v] == v && f2[v] == v ) con[i][p++] = v;
			}
			con[i].resize(p); m += p;
		}
		// printf( "cnt1_n = %lld, cnt1_m = %lld, cnt2_n = %lld, cnt2_m = %lld, m = %lld\n", cnt1_n, cnt1_m, cnt2_n, cnt2_m, m );
	}
	// nodes change their original ids
	printf( "Reordering...\n" );
	int *f = new int[n];
	get_order(con, n, f, rank_method, rank_threads, rank_max_minutes, max_hops);

	int *oid = new int[n], *nid = new int[n];
	for( int i = 0; i < n; ++i )
		oid[i] = f[i], nid[f[i]] = i;

	for( int i = 0; i < n; ++i ) {
		if(f1[i] != i) nid[i] = -nid[f1[i]]-1;
		if(f2[i] != i) nid[i] = MAXN + nid[f2[i]];
	}

	printf( "Creating adjacency list...\n" );
	int *dat = new int[m], *deg = new int[n], **adj = new int *[n];

	long long pos = 0;
	for( int i = 0; i < n; ++i ) {
		adj[i] = dat + pos;
		pos += (int) con[oid[i]].size();
	}
	memset( deg, 0, sizeof(int) * n );

	for( int i = 0; i < n; ++i ) {
		int ii = oid[i];
		for( int p = 0; p < (int) con[ii].size(); ++p ) {
			int jj = con[ii][p];
			int j = nid[jj];
			adj[j][deg[j]++] = i;
			// printf("j=%d, jj=%d, i=%d, ii=%d, deg=%d\n", j, jj, i, ii, deg[j]);
		}
	}

	// for( int i = 0; i < n; ++i ) {
	// 	printf( " i=%d, nid=%d\n", i, nid[i] );
	// }

	nowt = omp_get_wtime() - nowt;
	printf( "Creating Bin Time = %0.3lf secs\n", nowt );

	printf( "Saving binary...\n" );
	FILE *fout = fopen( (path + "graph-dis.bin").c_str(), "wb" );

	fwrite( &n, sizeof(int), 1, fout );
	fwrite( &m, sizeof(long long), 1, fout );
	fwrite( deg, sizeof(int), n, fout );
	fwrite( dat, sizeof(int), m, fout );
	fwrite( nid, sizeof(int), n, fout );
	fclose( fout );

	printf( "Created binary file, n = %d, m = %lld\n", n, m );
	delete[] adj; delete[] deg; delete[] dat; delete[] f; delete[] con; delete[] oid; delete[] nid;
	delete[] f1; delete[] f2;
}

#endif /* CORETREELABELLING_H_ */
