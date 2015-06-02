#ifndef WORKEROL_H
#define WORKEROL_H

#include "global_ol.h"
#include "utils/global.h"
#include "MessageBufferOL.h"
#include <string>
#include "utils/communication.h"
#include "utils/ydhdfs.h"
#include "hdfs.h"
#include "utils/Combiner.h"
#include "tools/msgtool.h"
#include "utils/Aggregator.h"//for DummyAgg


#include <unistd.h>
using namespace std;

//note an important barrier in run()

template <class VertexOLT, class AggregatorT = DummyAgg, class IndexT = char>//indexT is a per-worker index (e.g. InvIdx) ***
class WorkerOL {

	public:
		typedef vector<VertexOLT*> VertexContainer;
		typedef typename VertexContainer::iterator VertexIter;

		typedef typename VertexOLT::KeyType KeyT;
		typedef typename VertexOLT::MessageType MessageT;
		typedef typename VertexOLT::QueryType QueryT;
		typedef typename VertexOLT::HashType HashT;
		typedef typename VertexOLT::TaskT TaskT;
		typedef typename VertexOLT::QMap QMap;
		typedef typename QMap::iterator QMapIter;

		typedef typename AggregatorT::PartialType PartialT;
		typedef typename AggregatorT::FinalType FinalT;

		typedef MessageBufferOL<VertexOLT> MessageBufT;
		typedef typename MessageBufT::MessageContainerT MessageContainerT;
		typedef typename MessageBufT::Map Map;
		typedef typename MessageBufT::MapIter MapIter;

		HashT hash;
		VertexContainer vertexes;
		QMap queries;

		MessageBufT* message_buffer;
		Combiner<MessageT>* combiner;

		msg_queue_server* server;
		msg_queue_notifier* notifier;//ADDED FOR AUTO
		long type;
		int nxt_qid;
		int glob_step;

		const char* output_folder;
		char outpath[50];
		char qfile[10];

		bool use_agg;
		bool save_tag;

		IndexT index;//per-worker index***
		bool use_index;

		WorkerOL(bool agg_used = false, bool save_at_last = false, bool idx_used = false)
		{
			init_workers();
			use_agg = agg_used;
			save_tag = save_at_last;
			use_index = idx_used;//per-worker index***
			glob_step = 1;
			global_vertexes = &vertexes;
			active_queries = &queries;
			message_buffer = new MessageBufT;
			global_message_buffer = message_buffer;
			combiner = NULL;
			global_combiner = NULL;
			if (_my_rank == MASTER_RANK) 
			{	
				server=new msg_queue_server;
				notifier=new msg_queue_notifier;
			}
			nxt_qid=1;
			type = 1;
		}

		void setCombiner(Combiner<MessageT>* cb)
		{
			combiner = cb;
			global_combiner = cb;
		}

		~WorkerOL()
		{
			for(int i = 0; i < vertexes.size(); i++) delete vertexes[i];
			delete message_buffer;
			if (_my_rank == MASTER_RANK) 
			{
				delete server;
				delete notifier;
			}
			worker_finalize();
		}

		//==================================
		AggregatorT* get_aggregator()
		{
			TaskT& task=*(TaskT*)query_entry();
			return (AggregatorT*)(task.aggregator);
		}

		FinalT* get_agg()
		{
			TaskT& task=*(TaskT*)query_entry();
			return (FinalT*)(task.agg);
		}
		//==================================
		//sub-functions
		void sync_graph()
		{
			//ResetTimer(4);
			//set send buffer
			
			vector<VertexContainer> _loaded_parts(_num_workers);
			for (int i = 0; i < vertexes.size(); i++) {
				VertexOLT* v = vertexes[i];
				_loaded_parts[hash(v->id)].push_back(v);
			}
			
			//exchange vertices to add
			all_to_all(_loaded_parts);
			

			//delete sent vertices
			for (int i = 0; i < vertexes.size(); i++) {
				VertexOLT* v = vertexes[i];
				if (hash(v->id) != _my_rank)
					delete v;
			}
			vertexes.clear();
			
			//collect vertices to add
			for (int i = 0; i < _num_workers; i++) {
				vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
			}
			//StopTimer(4);
			//PrintTimer("Reduce Time",4);
		};

		//per-worker index***
		//index-construction UDFs
		virtual void load2Idx(char* line, IndexT& idx){}
		virtual void load2Idx(VertexOLT* v, int position, IndexT& idx){}
		virtual void idx_init(){}//if(use_index), the function will be called
		//functions for users to call in idx_init()
		void load_idx_from_file(char * idxpath)
		{
			hdfsFS fs = getHdfsFS();
			hdfsFile in = getRHandle(idxpath, fs);
			LineReader reader(fs, in);
			while (true) {
				reader.readLine();
				if (!reader.eof())
					load2Idx(reader.getLine(), index);
				else
					break;
			}
			hdfsCloseFile(fs, in);
			hdfsDisconnect(fs);
		}
		//----
		void load_idx_from_vertexes()
		{
			for(int i=0; i<vertexes.size(); i++)
			{
				VertexOLT* v = vertexes[i];
				load2Idx(v, i, index);
			}
		}
		//function for users to call in init(VertexContainer& vertex_vec)
		inline IndexT& idx(){ return index; }

		int compute_dump()
		{
			//cout << _my_rank << " I am compute_dump" << endl;
			QMapIter qit = queries.begin();
			while(qit != queries.end())
			{
				TaskT& task=qit->second;
				task.check_termination();
				if(task.superstep!=-1){//compute
					//set query environment
					set_qid(qit->first);
					set_query_entry(&task);
					//process this query
					task.start_another_superstep();
					
					//- aggregator init (must call init() after superstep is increased)
					
					AggregatorT* aggregator;
					if(use_agg && task.query[0] == TOPKNEIGHBORS_EARLIEST || task.query[0] == TOPKNEIGHBORS_FASTEST
					 || task.query[0] == TOPKNEIGHBORS_SHORTEST || task.query[0] == TOPKNEIGHBORS_LATEST)
					{
						aggregator = get_aggregator();
						aggregator->init(task.query[2]);
					}
					
					hash_set<int> active_set;
					task.move_active_vertices_to(active_set);
					for(hash_set<int>::iterator it=active_set.begin(); it!=active_set.end(); it++)
					{
						int pos=*it;
						VertexOLT* v=vertexes[pos];
						v->vertex_compute();
						//if(use_agg && task.query[0] == TOPKNEIGHBORS) aggregator->stepPartial(v);
						if(v->is_active()) task.activate(pos);
					}
					//agg
					if(use_agg && 
						task.query[0] == TOPKNEIGHBORS_EARLIEST || task.query[0] == TOPKNEIGHBORS_FASTEST
					 || task.query[0] == TOPKNEIGHBORS_SHORTEST || task.query[0] == TOPKNEIGHBORS_LATEST)
					{
						for(hash_set<int>::iterator it=task.created.begin(); it!=task.created.end(); it++)
						{
							aggregator->stepPartial(vertexes[*it]);
						}
					}
					
					if (task.superstep == 1 || task.restart == 1)
					{
						task.roundNum ++;
						task.restart = 0;
					}
					
					
					//aggregating
					if(use_agg && 
						task.query[0] == TOPKNEIGHBORS_EARLIEST || task.query[0] == TOPKNEIGHBORS_FASTEST
					 || task.query[0] == TOPKNEIGHBORS_SHORTEST || task.query[0] == TOPKNEIGHBORS_LATEST)
					{
						FinalT* agg = get_agg();
						if (_my_rank != MASTER_RANK) { //send partialT to aggregator
							//gathering PartialT
							PartialT* part = aggregator->finishPartial();
							slaveGather(*part);
							//scattering FinalT
							slaveBcast(*agg);
						} else {
							//gathering PartialT
							vector<PartialT*> parts(_num_workers);
							masterGather(parts);
							for (int i = 0; i < _num_workers; i++) {
								if (i != MASTER_RANK) {
									PartialT* part = parts[i];
									aggregator->stepFinal(part);
									delete part;
								}
							}
							//scattering FinalT
							FinalT* final = aggregator->finishFinal();
							*agg = *final;//deep copy
							masterBcast(*agg);
						}
					}
					//------
					++qit;
					/*
					task.check_termination();
					if (task.superstep == -1) return -1;
					*/
				}
				else//dump
				{
					//set query environment
					set_qid(qit->first);
					set_query_entry(&task);
					//cout << "cp1" << endl;
					
					
					/*
						for earliest arrival path using topChain
					*/
					if (task.query[0] == EARLIEST_QUERY_TOPCHAIN)
					{
						//cout << "cp1" << endl;
						//broadcast label from dst
						bool stop = 0;
						if (_my_rank == task.dstWorker)
						{
							/*
							cout << task.l << " " << task.r << endl;
							cout << task.m << endl;
							cout << task.ans << endl;
							cout << "superstep " << task.maxSuperstep << endl;
							*/
							if (task.l < 0) stop = 1;
							else{
								if (task.visit)
								{
									task.r = task.m-1;
									task.ans = task.m;
									if (task.l > task.r) {stop = 1;}
									else task.m = (task.l+task.r)/2;
								}
								else
								{
									//cannot visit
									task.l = task.m+1;
									if (task.l > task.r) {stop = 1;}
									else task.m = (task.l+task.r)/2;	
								}
							}
							/*
							cout << "test information" << endl;
							cout << task.ans << endl;
							cout << task.l << " " << task.r << " " << endl;
							*/
							//sleep(100000);
							task.visit = 0;
							if (!stop) //continue
							{
								int idx = task.m;
								//find dst
								QueryT& queryContainer = *get_query();
								int dst = queryContainer[2]; //queryType: 3, src, dst,
								int pos = get_vpos(dst);
								VertexOLT* v=vertexes[pos];
								//cout << "idx: " << idx << endl;
								task.dst_info[0] = (v->LinIn)[idx].size();
								for (int j = 0; j < task.dst_info[0]; ++ j) 
								{task.dst_info[2*j+1] = (v->LinIn)[idx][j].first ;task.dst_info[2*j+2] = (v->LinIn)[idx][j].second;}
						
								task.dst_info[2*labelSize+1] = (v->LoutIn)[idx].size();
								for (int j = 0; j < task.dst_info[2*labelSize+1]; ++ j) 
								{task.dst_info[j*2+2*labelSize+2] = (v->LoutIn)[idx][j].first;task.dst_info[j*2+2*labelSize+3] = (v->LoutIn)[idx][j].second;}
								task.dst_info[labelSize*4+2] = (v->topologicalLevelIn)[idx];
								task.dst_info[labelSize*4+3] = v->Vin[idx];
							}
							else //stop
							{
								if (task.ans == -1) //cannot find earliest arrival path
								{
									QueryT& queryContainer = *get_query();
									task.dst_info[0] = -inf;
									cout << "(" << queryContainer[1] << "," << queryContainer[2] << ") cannot visit" << endl;
									cout << "Number of Rounds: " << task.roundNum << endl;
								}
								else 
								{
									QueryT& queryContainer = *get_query();
									int dst = queryContainer[2]; //queryType: 3, src, dst,
									int pos = get_vpos(dst);
									VertexOLT* v=vertexes[pos];
									task.dst_info[0] = -v->Vin[task.ans];
									cout << "(" << queryContainer[1] << "," << queryContainer[2] << ") earliest arrival time is " << -task.dst_info[0] << endl; 
									cout << "Number of Rounds: " << task.roundNum << endl;
									
									v->qvalue()[0] = task.dst_info[0]; //can visit label
								}
							}
							
							sendBcast(stop, task.dstWorker);
							
							//put label into dst_info
							if (!stop) sendBcast(task.dst_info, task.dstWorker);
						}
						else
						{
							receiveBcast(stop, task.dstWorker);
							
							if (!stop) receiveBcast(task.dst_info, task.dstWorker);
						}
						
						if (!stop) //every vertex should contain at least one in-label
						{
							task.dst_Lin.resize(task.dst_info[0]);
							for (int j = 0; j < task.dst_info[0]; ++j) task.dst_Lin[j] = (make_pair(task.dst_info[j*2+1], task.dst_info[j*2+2]));
							task.dst_Lout.resize(task.dst_info[labelSize*2+1]);
							for (int j = 0; j < task.dst_info[labelSize*2+1]; ++j) task.dst_Lout[j] = (make_pair(task.dst_info[j*2+2*labelSize+2], task.dst_info[j*2+2*labelSize+3]));
							task.dst_topologicalLevel = task.dst_info[labelSize*4+2];
							task.dst_timeLabel = task.dst_info[labelSize*4+3];
							
							//superstep??
							task.active.clear();
							task_init();
							task.visit = 0;
							task.superstep = task.maxSuperstep;
							task.restart = 1;
							task.clearBits();
							++qit;
							continue;
						}
						else
						{
							//stop
						}
					}
					/*
						fastest query
					*/
					/*
					if (task.query[0] == FASTEST_QUERY_TOPCHAIN)
					{
						//cout << "cp1" << endl;
						//broadcast label from dst
						int stop = 0;
						if (_my_rank == task.dstWorker)
						{
							
							
							if (task.visit)
							{
								task.r = task.m-1;
								task.ans = task.m;
								if (task.l > task.r) {stop = 1;}
								else task.m = (task.l+task.r)/2;
							}
							else
							{
								//cannot visit
								task.l = task.m+1;
								if (task.l > task.r) {stop = 1;}
								else task.m = (task.l+task.r)/2;	
							}
							
							task.visit = 0;
							if (!stop) //continue
							{
								int idx = task.m;
								//find dst
								QueryT& queryContainer = *get_query();
								int dst = queryContainer[2]; //queryType: 3, src, dst,
								int pos = get_vpos(dst);
								VertexOLT* v=vertexes[pos];
								//cout << "idx: " << idx << endl;
								task.dst_info[0] = (v->LinIn)[idx].size();
								for (int j = 0; j < task.dst_info[0]; ++ j) 
								{task.dst_info[2*j+1] = (v->LinIn)[idx][j].first ;task.dst_info[2*j+2] = (v->LinIn)[idx][j].second;}
						
								task.dst_info[2*labelSize+1] = (v->LoutIn)[idx].size();
								for (int j = 0; j < task.dst_info[2*labelSize+1]; ++ j) 
								{task.dst_info[j*2+2*labelSize+2] = (v->LoutIn)[idx][j].first;task.dst_info[j*2+2*labelSize+3] = (v->LoutIn)[idx][j].second;}
								task.dst_info[labelSize*4+2] = (v->topologicalLevelIn)[idx];
								task.dst_info[labelSize*4+3] = v->Vin[idx];
							}
							else //stop
							{
								if (task.ans == -1) //cannot find earliest arrival path
								{
									QueryT& queryContainer = *get_query();
									task.dst_info[0] = -inf;
									cout << "(" << queryContainer[1] << "," << queryContainer[2] << ") cannot visit" << endl;
									cout << "Number of Rounds: " << task.roundNum << endl;
									stop = -1;
								}
								else 
								{
									QueryT& queryContainer = *get_query();
									int dst = queryContainer[2]; //queryType: 3, src, dst,
									int pos = get_vpos(dst);
									VertexOLT* v=vertexes[pos];
									task.dst_info[0] = -v->Vin[task.ans];
									cout << "(" << queryContainer[1] << "," << queryContainer[2] << ") earliest arrival time is " << -task.dst_info[0] << endl; 
									cout << "Number of Rounds: " << task.roundNum << endl;
									stop = -task.dst_info[0]; //stop carry information
								}
							}
							
							sendBcast(stop, task.dstWorker);
							
							//put label into dst_info
							if (!stop) sendBcast(task.dst_info, task.dstWorker);
						}
						else
						{
							receiveBcast(stop, task.dstWorker);
							
							if (!stop) receiveBcast(task.dst_info, task.dstWorker);
						}
						
						if (!stop) //every vertex should contain at least one in-label
						{
							task.dst_Lin.resize(task.dst_info[0]);
							for (int j = 0; j < task.dst_info[0]; ++j) task.dst_Lin[j] = (make_pair(task.dst_info[j*2+1], task.dst_info[j*2+2]));
							task.dst_Lout.resize(task.dst_info[labelSize*2+1]);
							for (int j = 0; j < task.dst_info[labelSize*2+1]; ++j) task.dst_Lout[j] = (make_pair(task.dst_info[j*2+2*labelSize+2], task.dst_info[j*2+2*labelSize+3]));
							task.dst_topologicalLevel = task.dst_info[labelSize*4+2];
							task.dst_timeLabel = task.dst_info[labelSize*4+3];
							
							//superstep??
							task.active.clear();
							task_init();
							task.visit = 0;
							task.superstep = task.maxSuperstep;
							task.restart = 1;
							task.clearBits();
							++qit;
							continue;
						}
						else
						{
							//stop: <0 cannot visit  >0 can visit
							
							bool fastestStop = 0;
							if (_my_rank == task.srcWorker)
							{
								task.rsrc --;
								if (task.lsrc > task.rsrc) fastestStop = 1;
								sendBcast(fastestStop, task.srcWorker);
								
								//cout << task.lsrc << " " << task.rsrc << endl;
							}
							else
							{
								receiveBcast(fastestStop, task.dstWorker);
							}
							
							
							if (!fastestStop)
							{
								task.active.clear();
								task_init();
								task.visit = 0;
								task.superstep = task.maxSuperstep;
								task.restart = 1;
								task.clearBits();
								task.roundNum = 0;
								++qit;
								
								if (_my_rank == task.dstWorker)
								{
									int dst = task.query[2];
									int pos = get_vpos(dst);
									assert(pos != -1); //assert
									VertexOLT* v=vertexes[pos];
									
									int t1, t2;
									t1 = 0; t2 = inf;
									if (task.query.size() == 5) {t1 = task.query[3]; t2 = task.query[4]; }
									map<int,int>::iterator it, it0;
									it0 = (v->mIn).lower_bound(t1);  //l
									it = (v->mIn).upper_bound(t2);   //r
									if (task.ans == -1)
									{
										if (it == (v->mIn).begin() || it0 == (v->mIn).end()) //check whether In is empty
										{
											task.dst_info[0] = -1;
											task.l = -1;
											task.r = -2;
										}
										else
										{
											it--;
											int idx = it->second; int idx0 = it0->second;
											if (idx0 <= idx){
												task.l = idx0;
												task.r = idx;
											}
											else
											{
												task.l = -1;
												task.r = -2;
												task.dst_info[0] = -1;
											}
										}
									}
									else
									{
										task.l = it0->second;
										task.r = task.ans - 1; //task.r can shrink
									}
									task.m = (task.l+task.r)/2;
									if (task.m >= 0){		
										int idx = task.m;
										task.dst_info[0] = (v->LinIn)[idx].size();
										for (int j = 0; j < task.dst_info[0]; ++ j) 
										{task.dst_info[2*j+1] = (v->LinIn)[idx][j].first ;task.dst_info[2*j+2] = (v->LinIn)[idx][j].second;}
					
										task.dst_info[2*labelSize+1] = (v->LoutIn)[idx].size();
										for (int j = 0; j < task.dst_info[2*labelSize+1]; ++ j) 
										{task.dst_info[j*2+2*labelSize+2] = (v->LoutIn)[idx][j].first;task.dst_info[j*2+2*labelSize+3] = (v->LoutIn)[idx][j].second;}
										task.dst_info[labelSize*4+2] = (v->topologicalLevelIn)[idx];
										task.dst_info[labelSize*4+3] = v->Vin[idx];
										
										sendBcast(task.dst_info, task.dstWorker);	
									}
									
									cout << "stop: " << stop << endl;
									cout << "task.m " << task.m << " " << v->Vin[task.m] << endl;
									cout << "task.ans " << task.ans << endl;
									cout << "task.l task.r: " << task.l << " " << task.r << endl;
									cout << "task.r: " << v->Vin[task.r] << endl;
									cout << "task.lsrc task.rsrc: " << task.lsrc << " " << task.rsrc << endl; 
									task.ans = -1;
								}
								else receiveBcast(task.dst_info, task.dstWorker);
								
								if (task.dst_info[0] != -1)
								{
					
									task.dst_Lin.resize(task.dst_info[0]);
									for (int j = 0; j < task.dst_info[0]; ++j) task.dst_Lin[j] = (make_pair(task.dst_info[j*2+1], task.dst_info[j*2+2]));
									task.dst_Lout.resize(task.dst_info[labelSize*2+1]);
									for (int j = 0; j < task.dst_info[labelSize*2+1]; ++j) task.dst_Lout[j] = (make_pair(task.dst_info[j*2+2*labelSize+2], task.dst_info[j*2+2*labelSize+3]));
									task.dst_topologicalLevel = task.dst_info[labelSize*4+2];
									task.dst_timeLabel = task.dst_info[labelSize*4+3];
					
								}
								
								continue;
							}
							else {}
							
						}
					}
					*/		
					
					//dump query result
					strcpy(outpath, output_folder);
					sprintf(qfile, "/query%d", qit->first);
					strcat(outpath, qfile);
					
					
					
					dump_partition_and_free_states(outpath);
					
					
					int accessed=all_sum(task.created.size());//--------------------------------------------
					if(_my_rank==MASTER_RANK)
					{
						double time=task.get_runtime();
						double rate=((double)accessed)/get_vnum();
						//cout << "accessed: " << accessed << endl;
						//--------------------------------------------
						cout<<"Q"<<qit->first<<" dumped, response time: "<<time<<" seconds, vertex access rate: "<<rate<<endl;
						cout<<"superstep #: " << task.maxSuperstep << endl;
						//--------------------------------------------
						//notifying the client that the query is processed
						sprintf(outpath, "%d %lf %lf %d", qit->first, time, rate, task.maxSuperstep);
						notifier->send_msg(type, outpath);
					}
					
					
					//free the query task
					if(use_agg && 
						task.query[0] == TOPKNEIGHBORS_EARLIEST || task.query[0] == TOPKNEIGHBORS_FASTEST
					 || task.query[0] == TOPKNEIGHBORS_SHORTEST || task.query[0] == TOPKNEIGHBORS_LATEST)
					{
						delete (AggregatorT*)task.aggregator;
						delete (FinalT*)task.agg;
					}
					
					queries.erase(qit++);
				}
				
			}
			//-----------------------------------
			//aggregating
			/*
			if(use_agg)
			{
				for(qit = queries.begin(); qit != queries.end(); qit++)
				{
					//set query environment
					TaskT& task=qit->second;
					set_qid(qit->first);
					set_query_entry(&task);
					AggregatorT* aggregator = get_aggregator();
					//------
					FinalT* agg = get_agg();
					if (_my_rank != MASTER_RANK) { //send partialT to aggregator
						//gathering PartialT
						PartialT* part = aggregator->finishPartial();
						slaveGather(*part);
						//scattering FinalT
						slaveBcast(*agg);
					} else {
						//gathering PartialT
						vector<PartialT*> parts(_num_workers);
						masterGather(parts);
						for (int i = 0; i < _num_workers; i++) {
							if (i != MASTER_RANK) {
								PartialT* part = parts[i];
								aggregator->stepFinal(part);
								delete part;
							}
						}
						//scattering FinalT
						FinalT* final = aggregator->finishFinal();
						*agg = *final;//deep copy
						masterBcast(*agg);
					}
				}
			}
			*/
			//cout << _my_rank << " compute completed." << endl;
			return 0;
		}

		//agg_sync() to be implemented here
		//note that aggregator maintains partial states, and need to be associated with "Task" !!!

		//=============================================
		//UDF: how to initialize active (created)
		virtual void init(VertexContainer& vertex_vec)=0;
		//it can call get_vpos(), activate();

		//functions to be called in UDF init():
		int get_vpos(KeyT vertex_id)
		{
			//return -1 if the vertex with the specified id is not found
			return message_buffer->get_vpos(vertex_id);
		}

		//functions to be called in UDF init():
		QueryT* get_query()//called in compute()
		{
			TaskT& task=*(TaskT*)query_entry();
			return &task.query;
		}

		//WorkerOL/UDF_init() calls activate() to add a vertex to active_set
		void activate(int vertex_position)
		{
			TaskT& task=*(TaskT*)query_entry();
			task.activate(vertex_position);
		}

		//system call
		virtual void task_init() = 0;
		/*
		void task_init()
		{
			//init(vertexes);
			
			QueryT& queryContainer = *get_query();
			//cout << "queryContainer: " << endl;
			//cout << queryContainer.size() << endl;
			//for (int i = 0; i < queryContainer.size(); ++ i) cout << queryContainer[i] << endl;
			if (queryContainer[0] == OUT_NEIGHBORS_QUERY || queryContainer[0] == IN_NEIGHBORS_QUERY 
			|| queryContainer[0] == REACHABILITY_QUERY || queryContainer[0] == REACHABILITY_QUERY_TOPCHAIN 
			|| queryContainer[0] == EARLIEST_QUERY_TOPCHAIN || queryContainer[0] == FASTEST_QUERY_TOPCHAIN
			|| queryContainer[0] == EARLIEST_QUERY || queryContainer[0] == LATEST_QUERY || queryContainer[0] == FASTEST_QUERY || queryContainer[0] == SHORTEST_QUERY
			|| queryContainer[0] == TOPKNEIGHBORS_EARLIEST || queryContainer[0] == TOPKNEIGHBORS_FASTEST
			|| queryContainer[0] == TOPKNEIGHBORS_SHORTEST || queryContainer[0] == TOPKNEIGHBORS_LATEST
			|| queryContainer[0] == KHOP_EARLIEST || queryContainer[0] == KHOP_FASTEST
			|| queryContainer[0] == KHOP_SHORTEST || queryContainer[0] == KHOP_LATEST
			)
			{
				//get neighbors: 1/2, v, (t1,t2)
				//reachability: 3/4, src, dst, (t1, t2)
				int src = queryContainer[1];
				int pos = get_vpos(src);
				if (pos != -1) activate(pos);
			}
			if (queryContainer[0] == ADDEDGE
			|| queryContainer[0] == INTERSECT
			|| queryContainer[0] == MIDDLE
			)
			{
				int src = queryContainer[1];
				int dst = queryContainer[2];
				int pos = get_vpos(src);
				if (pos != -1) activate(pos);
				pos = get_vpos(dst);
				if (pos != -1) activate(pos);
			}
			if (queryContainer[0] == TEST)
			{
				for (int i = 0; i < vertexes.size(); ++ i)
				{
					activate(get_vpos(vertexes[i]->id));
				}
			}
			
			//useCombiner
			if (queryContainer[0] == REACHABILITY_QUERY || queryContainer[0] == REACHABILITY_QUERY_TOPCHAIN 
			|| queryContainer[0] == EARLIEST_QUERY_TOPCHAIN || queryContainer[0] == FASTEST_QUERY_TOPCHAIN
			|| queryContainer[0] == EARLIEST_QUERY  || queryContainer[0] == LATEST_QUERY
			|| queryContainer[0] == TOPKNEIGHBORS_EARLIEST || queryContainer[0] == TOPKNEIGHBORS_LATEST
			|| queryContainer[0] == KHOP_EARLIEST || queryContainer[0] == KHOP_LATEST) 
			{
				TaskT& task=*(TaskT*)query_entry();
				task.useCombiner = 1;
			}
		}
		*/

		//------

		//UDF: how to dump a processed vertex
		virtual void dump(VertexOLT* vertex, BufferedWriter& writer)=0;
		//------
		//system call
		void dump_partition_and_free_states(char * outpath)
		{
			QueryT& queryContainer = *get_query();
			
			if (
				//path query
				queryContainer[0] == EARLIEST_QUERY || queryContainer[0] == LATEST_QUERY || 
				queryContainer[0] == FASTEST_QUERY  || queryContainer[0] == SHORTEST_QUERY || 
				//middle
				queryContainer[0] == MIDDLE || queryContainer[0] == INTERSECT
				//topk khop
				|| queryContainer[0] == TOPKNEIGHBORS_EARLIEST || queryContainer[0] == TOPKNEIGHBORS_FASTEST 
				|| queryContainer[0] == TOPKNEIGHBORS_SHORTEST || queryContainer[0] == TOPKNEIGHBORS_LATEST
				
				|| queryContainer[0] == KHOP_EARLIEST || queryContainer[0] == KHOP_FASTEST 
				|| queryContainer[0] == KHOP_SHORTEST || queryContainer[0] == KHOP_LATEST
				//neighbors
				|| queryContainer[0] == OUT_NEIGHBORS_QUERY || queryContainer[0] == IN_NEIGHBORS_QUERY
				//reachability
				|| queryContainer[0] == REACHABILITY_QUERY || queryContainer[0] == REACHABILITY_QUERY_TOPCHAIN
				|| queryContainer[0] == EARLIEST_QUERY_TOPCHAIN
			 	|| queryContainer[0] == TEST)
			{
			
				hdfsFS fs = getHdfsFS();
				BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

				TaskT& task=*(TaskT*)query_entry();
				for (hash_set<int>::iterator it = task.created.begin(); it != task.created.end(); it++) {
					VertexOLT* v = vertexes[*it];
					writer->check();
					dump(v, *writer);
					v->free();
				}
				delete writer;
				hdfsDisconnect(fs);
			}
			else
			{
				TaskT& task=*(TaskT*)query_entry();
				for (hash_set<int>::iterator it = task.created.begin(); it != task.created.end(); it++) {
					VertexOLT* v = vertexes[*it];
					v->free();
				}
			}
		}

		//------

		string path2save;
		void set_file2save(string path)
		{
			path2save=path;
		}

		//UDF: how to dump a vertex's NQValue when the server is turned down
		virtual void save(VertexOLT* vertex, BufferedWriter& writer){};//default implemetation is doing nothing

		//system call
		void save_vertices()
		{
			hdfsFS fs = getHdfsFS();
			BufferedWriter* writer = new BufferedWriter(path2save.c_str(), fs, _my_rank);

			for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
				writer->check();
				save(*it, *writer);
			}
			delete writer;
			hdfsDisconnect(fs);
		}

		//=============================================

		//user-defined graphLoader ==============================
		virtual VertexOLT* toVertex(char* line) = 0; //this is what user specifies!!!!!!

		void load_vertex(VertexOLT* v)
		{ //called by load_graph
			vertexes.push_back(v);
		}

		void load_graph(const char* inpath)
		{
			hdfsFS fs = getHdfsFS();
			hdfsFile in = getRHandle(inpath, fs);
			LineReader reader(fs, in);
			while (true) {
				reader.readLine();
				if (!reader.eof())
					load_vertex(toVertex(reader.getLine()));
				else
					break;
			}
			hdfsCloseFile(fs, in);
			hdfsDisconnect(fs);
			//cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
		}
		//=======================================================

		//user-defined qyeryLoader ==============================
		virtual QueryT toQuery(char* line) = 0; //this is what user specifies!!!!!!

		struct qinfo
		{
			int qid;
			QueryT q;

			qinfo(){}

			qinfo(int qid, QueryT q)
			{
				this->qid=qid;
				this->q=q;
			}

			friend ibinstream& operator<<(ibinstream& m, const qinfo& v)
			{
				m << v.qid;
				m << v.q;
				return m;
			}

			friend obinstream& operator>>(obinstream& m, qinfo& v)
			{
				m >> v.qid;
				m >> v.q;
				return m;
			}
		};

		bool update_tasks()//return false to shut_down server
		{
			if (queries.size() > 0) return 1;
			
			vector<qinfo> new_queries;
			if (_my_rank == MASTER_RANK)
			{
				long type=1;
				if(queries.size()>0)//has tasks running, do not wait till there are new queries
				{
					while(server->recv_msg(type))
					{
						char* msg=server->get_msg();
						cout<<"Q"<<nxt_qid<<": "<<msg<<endl;
						if(strcmp(msg, "server_exit")==0)
						{
							new_queries.clear();
							qinfo qentry(-1, QueryT());
							new_queries.push_back(qentry);
							masterBcast(new_queries);
							if(save_tag)
							{
								ResetTimer(WORKER_TIMER);
								save_vertices();
								StopTimer(WORKER_TIMER);
								PrintTimer("Vertices Saved, Time", WORKER_TIMER);
							}
							return false;
						}
						else
						{
							QueryT q=toQuery(msg);
							qinfo qentry(nxt_qid, q);
							new_queries.push_back(qentry);
							TaskT& task=queries[nxt_qid]=TaskT(q);
							if(use_agg)
							{
								task.aggregator=new AggregatorT;
								task.agg=new FinalT;
							}
							//create empty output_folder {
							strcpy(outpath, output_folder);
							sprintf(qfile, "/query%d", nxt_qid);
							strcat(outpath, qfile);
							dirCreate(outpath);
							//} create empty output_folder
							//set query environment
							set_qid(nxt_qid);
							set_query_entry(&task);
							//init active vertices
							task_init();
							//----
							nxt_qid++;
						}
					}
				}
				else//has no running task
				{
					while(server->recv_msg(type) == false);//busy waiting if server gets no query msg
					do{
						char* msg=server->get_msg();
						cout<<"Q"<<nxt_qid<<": "<<msg<<endl;
						if(strcmp(msg, "server_exit")==0)
						{
							new_queries.clear();
							qinfo qentry(-1, QueryT());
							new_queries.push_back(qentry);
							masterBcast(new_queries);
							if(save_tag)
							{
								ResetTimer(WORKER_TIMER);
								save_vertices();
								StopTimer(WORKER_TIMER);
								PrintTimer("Vertices Saved, Time", WORKER_TIMER);
							}
							return false;
						}
						else
						{
							QueryT q=toQuery(msg);
							qinfo qentry(nxt_qid, q);
							new_queries.push_back(qentry);
							TaskT& task=queries[nxt_qid]=TaskT(q);
							if(use_agg)
							{
								task.aggregator=new AggregatorT;
								task.agg=new FinalT;
							}
							//create empty output_folder {
							strcpy(outpath, output_folder);
							sprintf(qfile, "/query%d", nxt_qid);
							strcat(outpath, qfile);
							dirCreate(outpath);
							//} create empty output_folder
							//set query environment
							set_qid(nxt_qid);
							set_query_entry(&task);
							//init active vertices
							task_init();
							//----
							nxt_qid++;
						}
					}while(server->recv_msg(type));
				}
				masterBcast(new_queries);

				//broadcast dst's label
				bcastLabel(new_queries);
			}
			else
			{
				slaveBcast(new_queries);
				if(new_queries.size()==1 && new_queries[0].qid==-1)
				{
					if(save_tag)
					{
						ResetTimer(WORKER_TIMER);
						save_vertices();
						StopTimer(WORKER_TIMER);
						PrintTimer("Vertices Saved, Time", WORKER_TIMER);
					}
					return false;
				}
				else
				{
					for(int i=0; i<new_queries.size(); i++)
					{
						qinfo& info=new_queries[i];
						TaskT& task=queries[info.qid]=TaskT(info.q);
						if(use_agg)
						{
							task.aggregator=new AggregatorT;
							task.agg=new FinalT;
						}
						//set query environment
						set_qid(info.qid);
						set_query_entry(&task);
						//init active vertices
						task_init();
					}
					//broadcast dst's label
					bcastLabel(new_queries);
				}
			}
			return true;
		}
		
		void bcastLabel(vector<qinfo>& new_queries)
		{
			for (int i = 0; i < new_queries.size(); ++ i)
			{
				qinfo& info = new_queries[i];
				if (info.q[0] != REACHABILITY_QUERY_TOPCHAIN && info.q[0] != EARLIEST_QUERY_TOPCHAIN && info.q[0] != FASTEST_QUERY_TOPCHAIN) continue; //do not need to use topChain
				TaskT& task = queries[info.qid];
				//set query environment
				set_qid(info.qid);
				set_query_entry(&task);
				
				//find dst
				QueryT& queryContainer = *get_query();
				int dst = queryContainer[2]; //queryType: 3, src, dst,
				int pos = get_vpos(dst);
				//message type
				task.dst_info.resize(labelSize*4+3 + 1);
				// in:(int,int)... out:(int,int)... topologicalLevel
				if (pos != -1)
				{
					//task.dst_info[0] = _my_rank;
					
					VertexOLT* v=vertexes[pos];
					int t1, t2;
					t1 = 0; t2 = inf;
					if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
					map<int,int>::iterator it, it0;
					it0 = (v->mIn).lower_bound(t1);  //l
					it = (v->mIn).upper_bound(t2);   //r
					if (it == (v->mIn).begin() || it0 == (v->mIn).end()) //check whether In is empty
					{
						task.dst_info[0] = -1;
						task.l = -1;
						task.r = -2;
					}
					else
					{
						it --;
						int idx = it->second;
						int idx0 = it0->second;
						if (idx0 <= idx){
							task.l = idx0;
							task.r = idx;
							task.m = (task.l+task.r)/2;
							if (queryContainer[0] == EARLIEST_QUERY_TOPCHAIN
							|| queryContainer[0] == FASTEST_QUERY_TOPCHAIN) idx = (idx+idx0)/2;
							//cout << "l r: " << task.l << " " << task.r << endl; 
							//cout << "idx: " << idx << endl;
							task.dst_info[0] = (v->LinIn)[idx].size();
							for (int j = 0; j < task.dst_info[0]; ++ j) 
							{task.dst_info[2*j+1] = (v->LinIn)[idx][j].first ;task.dst_info[2*j+2] = (v->LinIn)[idx][j].second;}
						
							task.dst_info[2*labelSize+1] = (v->LoutIn)[idx].size();
							for (int j = 0; j < task.dst_info[2*labelSize+1]; ++ j) 
							{task.dst_info[j*2+2*labelSize+2] = (v->LoutIn)[idx][j].first;task.dst_info[j*2+2*labelSize+3] = (v->LoutIn)[idx][j].second;}
							task.dst_info[labelSize*4+2] = (v->topologicalLevelIn)[idx];
							
							task.dst_info[labelSize*4+3] = v->Vin[idx]; //time label
							/*
							for (int j = 0; j < labelSize*4+4; ++ j) cout << task.dst_info[j] << " ";
							cout << endl;
							*/
						}
						else 
						{
							task.dst_info[0] = -1;
							task.l = -1;
							task.r = -2;
						}
					}
					/*
					for (int j = 0; j < task.dst_info.size(); ++ j) cout << task.dst_info[j] << " ";
					cout << endl;
					*/
				}
				
				//send to master		
				int dstWorker;
				if (_my_rank == MASTER_RANK)
				{
					vector<int> workerInfo(_num_workers);
					//get dstWorker from others
					masterGather(workerInfo);
					dstWorker = MASTER_RANK;
					for (int i = 0; i < workerInfo.size(); ++ i)
					{
						if (i != _my_rank && workerInfo[i]!=-1) {dstWorker = workerInfo[i]; break;}
					}
					//bcastWorker to others
					masterBcast(dstWorker);
				}
				else
				{
					int toSend;
					if (pos == -1) toSend = -1;
					else toSend = _my_rank;
					//send to master
					slaveGather(toSend);
					//receive from master
					slaveBcast(dstWorker);
				}
				//set dstWorker
				task.dstWorker = dstWorker;
				
				if (pos != -1)
				{
					sendBcast(task.dst_info, dstWorker);
				}
				else
				{
					receiveBcast(task.dst_info, dstWorker);
				}
				
				if (task.dst_info[0] != -1)
				{
					
					task.dst_Lin.resize(task.dst_info[0]);
					for (int j = 0; j < task.dst_info[0]; ++j) task.dst_Lin[j] = (make_pair(task.dst_info[j*2+1], task.dst_info[j*2+2]));
					task.dst_Lout.resize(task.dst_info[labelSize*2+1]);
					for (int j = 0; j < task.dst_info[labelSize*2+1]; ++j) task.dst_Lout[j] = (make_pair(task.dst_info[j*2+2*labelSize+2], task.dst_info[j*2+2*labelSize+3]));
					task.dst_topologicalLevel = task.dst_info[labelSize*4+2];
					task.dst_timeLabel = task.dst_info[labelSize*4+3];
				}
				/*
				cout << "------" << endl;
				cout << "id " << _my_rank << endl;
				cout << "LinSize " << task.dst_Lin.size() << endl;
				cout << "LoutSize " << task.dst_Lout.size() << endl;
				cout << "Lin " << endl;
				for (int j = 0; j < task.dst_Lin.size(); ++ j) cout << task.dst_Lin[j].first << " " << task.dst_Lin[j].second << endl;
				cout << "Lout" << endl;
				for (int j = 0; j < task.dst_Lout.size(); ++ j) cout << task.dst_Lout[j].first << " " << task.dst_Lout[j].second << endl;
				cout << "topologicalLevel: " << task.dst_topologicalLevel << endl;
				*/
				
				
				//get srcWorker for fastest query
				/*
				if (info.q[0] == FASTEST_QUERY_TOPCHAIN) {
					int srcWorker;
					QueryT& queryContainer = *get_query();
					int src = queryContainer[1]; // src;
					int pos = get_vpos(src);
					if (pos != -1)
					{
						VertexOLT* v = vertexes[pos];
						int t1, t2;
						t1 = 0; t2 = inf;
						if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
						map<int, int>::iterator it, it0;
						it0 = (v->mOut).lower_bound(t1);
						it = (v->mOut).upper_bound(t2);
						if (it == (v->mOut).begin() || it0 == (v->mOut).end())
						{
							task.lsrc = -1;
							task.rsrc = -2;
						}
						else
						{
							it --;
							int idx = it->second;
							int idx0 = it0->second;
							if (idx0 <= idx)
							{
								task.lsrc = idx0;
								task.rsrc = idx;
							}
							else 
							{
								task.lsrc = -1;
								task.rsrc = -2;
							}
						}
					}
					if (_my_rank == MASTER_RANK)
					{
						vector<int> workerInfo(_num_workers);
						masterGather(workerInfo);
						srcWorker = MASTER_RANK;
						for (int i = 0; i < workerInfo.size(); ++ i)
						{
							if (i != _my_rank && workerInfo[i] != -1) {srcWorker = workerInfo[i];break;}
						}
						masterBcast(srcWorker);
					}
					else
					{
						int toSend;
						if (pos == -1) toSend = -1;
						else toSend = _my_rank;
					
						slaveGather(toSend);
						slaveBcast(srcWorker);
					}
					task.srcWorker = srcWorker;
				}
				*/
			}
			//cout << _my_rank << " bcast done" << endl;
		}
		
		
		//================================================

		// run the worker
		void run(const WorkerParams& params)
		{
			//check path + init
			if (_my_rank == MASTER_RANK) {
				if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
					exit(-1);
				if(save_tag)
				{
					if (outDirCheck(path2save.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1) exit(-1);
				}
			}
			output_folder = params.output_path.c_str();
			init_timers();

			//dispatch splits
			ResetTimer(WORKER_TIMER);
			vector<vector<string> >* arrangement;
			if (_my_rank == MASTER_RANK) {
				arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
				masterScatter(*arrangement);
				vector<string>& assignedSplits = (*arrangement)[0];
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
				delete arrangement;
			}
			else {
				vector<string> assignedSplits;
				slaveScatter(assignedSplits);
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
			}

			//send vertices according to hash_id (reduce)
			
			sync_graph();
			message_buffer->init(vertexes);
			//barrier for data loading
			worker_barrier();
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time", WORKER_TIMER);

			//per-worker index***
			if(use_index)
			{
				init_timers();
				idx_init();
				worker_barrier();
				StopTimer(WORKER_TIMER);
				PrintTimer("Indexing Time", WORKER_TIMER);
			}
			//per-worker index***

			get_vnum() = all_sum(vertexes.size());//vertex addition/deletion not supported right now
			//=========================================================
			
			
			int prePhase = 4;
			if (_my_rank==MASTER_RANK) cout << "begin preCompute" << endl;
			for (int i = 1; i <= prePhase; ++ i)
			{
				//if (_my_rank==MASTER_RANK) cout << "phaseNum: " << i << endl;
				preCompute(i);
				
				//no index
				//break;
			}
			//sleep(100000);
			
			
			//global_compute();
			int taskCounter = 0;
			init_timers();
			while(1)
			{
			
				if (taskCounter < task_list.size()) getNextTask(taskCounter);
				else 
				{
					bool tmp = update_tasks();
					if (!tmp) break;
				}
				
				
				ResetTimer(WORKER_TIMER);
				ResetTimer(4);
				int ret = compute_dump();
				if (ret != -1)
				{
					message_buffer->combine();
					message_buffer->sync_messages();
				}
				//--------------------------------
				worker_barrier();//VERY IMPORTANT
				//in sync_messages(), master sends to other workers, and it should be before update_tasks(), where master also sends to other workers
				//--------------------------------
				StopTimer(WORKER_TIMER);
				
				if (_my_rank == MASTER_RANK){
					cout<<"---------------------------------------"<<endl;
					cout<<"Global clock tick #: "<<glob_step<<endl;
					glob_step++;
					cout<<"Time Elapsed: "<<get_timer(WORKER_TIMER)<<" seconds"<<endl;
				}
			}
		}

		//================================================
		
		void preCompute(int phaseNum)
		{
			//global_step_num = 0;
			
			int phaseSuperstep = 0;
			QueryT q;
			q.push_back(PRECOMPUTE);
			TaskT& task = queries[nxt_qid] = TaskT(q);
			//set query environment
			set_qid(nxt_qid);
			set_query_entry(&task);
			nxt_qid++;
			QMapIter qit = queries.begin();
			//activate
			for (int i = 0; i < vertexes.size(); ++ i)
			{
				activate(get_vpos(vertexes[i]->id));
			}
			while(1)
			{
				task.check_termination();
				if (_my_rank==MASTER_RANK) cout << "superstep: " << task.superstep << endl;
				if (task.superstep!=-1)
				{
					phaseSuperstep ++;
					//set_query_entry(&task);
					task.start_another_superstep();
					hash_set<int> active_set;
					task.move_active_vertices_to(active_set);
					/*
					int i = 0;
					int total = active_set.size();
					int interval = total/100;
					int cur = 0;
					*/
					for(hash_set<int>::iterator it=active_set.begin(); it!=active_set.end(); it++)
					{
						int pos=*it;
						VertexOLT* v=vertexes[pos];
						v->vertex_pre_compute(phaseNum);
						
						//if(v->is_active()) task.activate(pos);
						/*
						i ++;
						if (_my_rank == 0 && phaseNum == 3 && i >= cur)
						{
							cur += interval;
							cout << cur/interval << endl;
						}
						*/
					}
					
					//all the vertexes should be active in phase1 superstep1
					if (phaseNum == 1 && task.superstep==1)
					{
						for (int i = 0; i < vertexes.size(); ++ i) activate(get_vpos(vertexes[i]->id));
					}
				}
				else//dump
				{
					if (_my_rank==MASTER_RANK)
					{
						cout << "preCompute phaseNum: " << phaseNum << " done!" << endl;
						double time = task.get_runtime();
						cout << "time used: " << time << " seconds" << endl;
						cout << "total superstep: " << phaseSuperstep << endl;
						cout << "-------------------------------" << endl;
					}
					for (int i = 0; i < vertexes.size(); ++ i)
					{
						vertexes[i]->free();
					}
					queries.erase(qit++);
					break;
				}
				//test
				//worker_barrier();
				//if (_my_rank == 0) cout << "done! waiting for messages" << endl;
				
				//message_buffer->combine();
				message_buffer->sync_messages();
				//--------------------------------
				worker_barrier();
			}
			//sleep(10000000);
		}
		
		vector<string> task_list;
		virtual void global_compute() = 0;
		void call_operator(string s)
		{
			task_list.push_back(s);
		}
		
		
		void getNextTask(int& taskCounter)
		{
			if (queries.size() > 0) return;
			char *msg = new char[task_list[taskCounter].length()+1];
			strcpy(msg, task_list[taskCounter].c_str());
			
			if (_my_rank == MASTER_RANK) cout<<"Q"<<nxt_qid<<": "<<msg<<endl;
			
			vector<qinfo> new_queries;
			
			QueryT q=toQuery(msg);
			qinfo qentry(nxt_qid, q);
			new_queries.push_back(qentry);
			TaskT& task=queries[nxt_qid]=TaskT(q);
			if(use_agg)
			{
				task.aggregator=new AggregatorT;
				task.agg=new FinalT;
			}
			//create empty output_folder {
			if (_my_rank == MASTER_RANK) 
			{
				strcpy(outpath, output_folder);
				sprintf(qfile, "/query%d", nxt_qid);
				strcat(outpath, qfile);
				dirCreate(outpath);
			}
			//} create empty output_folder
			//set query environment
			set_qid(nxt_qid);
			set_query_entry(&task);
			//init active vertices
			task_init();
			//----
			nxt_qid++;
			
			
			bcastLabel(new_queries);
			

			taskCounter++;
			delete [] msg;
		}

		// run the worker
		/*
		void run(const MultiInputParams& params)
		{
			//check path + init
			if (_my_rank == MASTER_RANK) {
				if (dirCheck(params.input_paths, params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
					exit(-1);
				if(save_tag)
				{
					if (outDirCheck(path2save.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1) exit(-1);
				}
			}
			output_folder = params.output_path.c_str();
			init_timers();

			//dispatch splits
			ResetTimer(WORKER_TIMER);
			vector<vector<string> >* arrangement;
			if (_my_rank == MASTER_RANK) {
				arrangement = params.native_dispatcher ? dispatchLocality(params.input_paths) : dispatchRan(params.input_paths);
				masterScatter(*arrangement);
				vector<string>& assignedSplits = (*arrangement)[0];
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
				delete arrangement;
			} else {
				vector<string> assignedSplits;
				slaveScatter(assignedSplits);
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
			}

			//send vertices according to hash_id (reduce)
			sync_graph();
			message_buffer->init(vertexes);
			//barrier for data loading
			worker_barrier();
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time", WORKER_TIMER);

			//per-worker index***
			if(use_index)
			{
				init_timers();
				idx_init();
				worker_barrier();
				StopTimer(WORKER_TIMER);
				PrintTimer("Indexing Time", WORKER_TIMER);
			}
			//per-worker index***

			get_vnum() = all_sum(vertexes.size());//vertex addition/deletion not supported right now
			//=========================================================
			
			
			preCompute();
			
			init_timers();
			while(update_tasks())
			{
				ResetTimer(WORKER_TIMER);
				ResetTimer(4);
				compute_dump();
				message_buffer->combine();
				message_buffer->sync_messages();
				//--------------------------------
				worker_barrier();//VERY IMPORTANT
				//in sync_messages(), master sends to other workers, and it should be before update_tasks(), where master also sends to other workers
				//--------------------------------
				StopTimer(WORKER_TIMER);
				if (_my_rank == MASTER_RANK){
					cout<<"---------------------------------------"<<endl;
					cout<<"Global clock tick #: "<<glob_step<<endl;
					glob_step++;
					cout<<"Time Elapsed: "<<get_timer(WORKER_TIMER)<<" seconds"<<endl;
				}
			}
		}
		*/
};

#endif
