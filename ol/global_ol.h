#ifndef GLOBAL_OL_H
#define GLOBAL_OL_H

#include <stddef.h>

//-----------------------------------------
//Global vertex_set
void* global_vertexes = NULL; //type = vector<VertexOL*>

inline void set_vertexes(void* vertexes)
{
	global_vertexes = vertexes;
}

inline void* get_vertexes()
{
    return global_vertexes;
}

//-----------------------------------------
//WorkerOL should ensure "active_queries" up-to-date
void* active_queries = NULL; //type = hash_map<qid, Task>

inline void set_active_queries(void* aqs)
{
	active_queries = aqs;
}

inline void* get_active_queries()
{
    return active_queries;
}

//-----------------------------------------
//to make the query currently being processed transparent to vertex.compute()
//WorkerOL should ensure "global_query_id" and "global_query" up-to-date
int global_query_id;

inline int query_id()
{
    return global_query_id;
}

inline void set_qid(int qid)
{
    global_query_id=qid;
}

void* global_query; //type = Task

inline void* query_entry()
{
    return global_query;
}

inline void set_query_entry(void* q)
{
	global_query=q;
}
//-----------------------------------------


//queryNotation:
#define PRECOMPUTE 0
//online query
#define OUT_NEIGHBORS_QUERY 1
#define IN_NEIGHBORS_QUERY 2
#define REACHABILITY_QUERY 3
#define REACHABILITY_QUERY_TOPCHAIN 4
#define EARLIEST_QUERY_TOPCHAIN 5
#define FASTEST_QUERY_TOPCHAIN 6
//analytic query
#define EARLIEST_QUERY 7
#define FASTEST_QUERY 8
#define SHORTEST_QUERY 9
#define LATEST_QUERY 10
//update
#define ADDEDGE 11
//applications
#define CLOSENESS 21
#define TOPKNEIGHBORS_EARLIEST 22
#define TOPKNEIGHBORS_FASTEST 23
#define TOPKNEIGHBORS_SHORTEST 24
#define TOPKNEIGHBORS_LATEST 25

#define KHOP_EARLIEST 26
#define KHOP_FASTEST 27
#define KHOP_SHORTEST 28
#define KHOP_LATEST 29

#define INTERSECT 31
#define MIDDLE 32
//test
#define TEST -1
#define labelSize 2
const int inf = 1e9;
#endif
