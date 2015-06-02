#include <ol/pregel-ol-dev.h>
#include <utils/type.h>
#include <sstream>
#include <algorithm>
#include <set>
#include <map>
#include <queue>
#include <assert.h>
string in_path = "/yuzhen/toyP";
//string in_path = "/yuzhen/sociopatterns";
//string in_path = "/yuzhen/editP";
string out_path = "/yuzhen/output";
bool use_combiner = true;


struct vertexValue {
    vector<vector<pair<int,int> > > tw; //(t,w)
    vector<int> neighbors;
   
};

ibinstream& operator<<(ibinstream& m, const vertexValue& v)
{
    m << v.tw;
    m << v.neighbors;
    return m;
}
ibinstream & operator<<(ibinstream & m, const pair<int,int>& p) 
{m << p.first << p.second; return m;}
obinstream& operator>>(obinstream& m, vertexValue& v)
{
    m >> v.tw;
    m >> v.neighbors;
    return m;
}
obinstream & operator>>(obinstream & m, pair<int,int>& p)
{m >> p.first >> p.second; return m;}

struct vw
{
	int v, w;
};

bool cmp2(const pair<int,int>& p1, const pair<int,int>& p2)
{
	if (p1.first == p2.first) return p1.second > p2.second;
	return p1.first < p2.first;
}

//idType, qvalueType, nqvalueType, messageType, queryType
class temporalVertex : public VertexOL<VertexID, vector<int>, vertexValue, vector<int>, vector<int> >{
public:
	
	std::map<int,int> mOut;
	int countOut;
	vector<int> Vout; //store time
	vector<vector<vw> > VoutNeighbors; //store neighbors
	
	std::map<int,int> mIn;
	int countIn;
	vector<int> Vin;
	vector<vector<vw> > VinNeighbors;
	
	
	vector<int> toOut;
	vector<int> toIn;
	//topological level
	vector<int> topologicalLevelIn;
	vector<int> topologicalLevelOut;
	vector<int> inDegreeIn;
	vector<int> inDegreeOut;
	vector<int> countinDegreeIn;
	vector<int> countinDegreeOut;
	
	//topChain
	//int labelSize; global
	vector<vector<pair<int,int> > > LinIn;
	vector<vector<pair<int,int> > > LinOut;
	vector<vector<pair<int,int> > > LoutIn;
	vector<vector<pair<int,int> > > LoutOut;
	
	vector<vector<pair<int,int> > > LtinIn;
	//vector<vector<pair<int,int> > > LtinOut;
	//vector<vector<pair<int,int> > > LtoutIn;
	vector<vector<pair<int,int> > > LtoutOut;
	
	vector<vector<pair<int,int> > > LcinIn;
	vector<vector<pair<int,int> > > LcinOut;
	vector<vector<pair<int,int> > > LcoutIn;
	vector<vector<pair<int,int> > > LcoutOut;
	
	//fastest path query
	vector<int> latestArrivalTime;
	vector<int> lastOut;

	//Step 5.1: define UDF1: query -> vertex's query-specific init state
    virtual vector<int> init_value(vector<int>& query) //QValueT init_value(QueryT& query
    {
    	/*
        if (id == query.v1)
            return 0;
        else
            return INT_MAX;
        */
        vector<int>& queryContainer = *get_query();
        vector<int> ret;
        if (queryContainer[0] == REACHABILITY_QUERY || queryContainer[0] == REACHABILITY_QUERY_TOPCHAIN 
        || queryContainer[0] == EARLIEST_QUERY_TOPCHAIN || queryContainer[0] == FASTEST_QUERY_TOPCHAIN
        || queryContainer[0] == EARLIEST_QUERY  || queryContainer[0] == FASTEST_QUERY || queryContainer[0] == SHORTEST_QUERY
        || queryContainer[0] == TOPKNEIGHBORS_EARLIEST || queryContainer[0] == TOPKNEIGHBORS_FASTEST
        || queryContainer[0] == TOPKNEIGHBORS_SHORTEST
        || queryContainer[0] == KHOP_EARLIEST || queryContainer[0] == KHOP_FASTEST
        || queryContainer[0] == KHOP_SHORTEST 
        
        || queryContainer[0] == INTERSECT
        || queryContainer[0] == MIDDLE
        
        )
        {
        	//if (Vout.size() > 0) ret.push_back(Vout[Vout.size()-1]+1); //lastT
        	//else ret.push_back(-1);
        	ret.push_back(inf);
        }
        if (queryContainer[0] == LATEST_QUERY || queryContainer[0] == TOPKNEIGHBORS_LATEST
        || queryContainer[0] == KHOP_LATEST) ret.push_back(0);
        
        if (queryContainer[0] == FASTEST_QUERY || queryContainer[0] == TOPKNEIGHBORS_FASTEST
        || queryContainer[0] == KHOP_FASTEST) 
        {
        	latestArrivalTime.resize(0);latestArrivalTime.resize(Vin.size(), -1);
        	lastOut.resize(0); lastOut.resize(Vout.size(), -1);
        }
        else if (queryContainer[0] == SHORTEST_QUERY || queryContainer[0] == TOPKNEIGHBORS_SHORTEST
        || queryContainer[0] == KHOP_SHORTEST) 
        {
        	latestArrivalTime.resize(0);latestArrivalTime.resize(Vin.size(), inf);
        	lastOut.resize(0); lastOut.resize(Vout.size(), inf);
        }
        
        
        return ret;
    }
    virtual void compute(MessageContainer& messages);
    
    
    
    int labelCompare(int idx) //-1: cannot reach; 1: reach; 0:cannot answer
    {
    	TaskT& task=*(TaskT*)query_entry();
    	
    	//timeLabel
    	if (Vout[idx] >= task.dst_timeLabel) return -1;
    	
    	//topo
    	if (topologicalLevelOut[idx] >= task.dst_topologicalLevel) return -1;
    	
    	//topoChain
    	
    	int p1, p2;
    	
    	p1 = p2 = 0;
    	while(p1 < LoutOut[idx].size() && p2 < task.dst_Lout.size() )
    	{
    		if (LoutOut[idx][p1].first == task.dst_Lout[p2].first)
    		{
    			if (LoutOut[idx][p1].second > task.dst_Lout[p2].second) return -1;
    			p1++; p2++;
    		}
    		else if (LoutOut[idx][p1].first < task.dst_Lout[p2].first) p1++;
    		else return -1;    		
    	}
    	
    	p1 = p2 = 0;
    	while(p1 < LinOut[idx].size() && p2 < task.dst_Lin.size() )
    	{
    		if (LinOut[idx][p1].first == task.dst_Lin[p2].first)
    		{
    			if (LinOut[idx][p1].second > task.dst_Lin[p2].second) return -1;
    			p1 ++; p2++;
    		}
    		else if (LinOut[idx][p1].first > task.dst_Lin[p2].first) p2++;
    		else return -1;
    	}
    	
    	//intersect
    	
    	p1 = p2 = 0;
    	while(p1 < LoutOut[idx].size() && p2 < task.dst_Lin.size() )
    	{
    		if (LoutOut[idx][p1].first == task.dst_Lin[p2].first && LoutOut[idx][p1].second <= task.dst_Lin[p2].second) return 1;
    		else if (LoutOut[idx][p1].first == task.dst_Lin[p2].first) {p1++;p2++;}
    		else if (LoutOut[idx][p1].first < task.dst_Lin[p2].first) p1++;
    		else p2++;
    	}
    	
    	return 0;
    }
    
    void mergeOut(vector<pair<int,int> >& final, vector<pair<int,int> >& v1, vector<pair<int,int> >& v2, vector<pair<int,int> >& store, int k)
    {
    	//merge v1, v2, store in tmp, merge tmp, final store in final, keep change in store
    	vector<pair<int,int> > tmp;
    	int p1,p2;
    	p1 = p2 = 0;
    	while(p1 < v1.size() || p2 < v2.size())
    	{
    		if (p1 == v1.size() || (p2!=v2.size()&&v2[p2].first < v1[p1].first)) {tmp.push_back(v2[p2]); p2++; }
    		else if (p2 == v2.size() || v1[p1].first < v2[p2].first) {tmp.push_back(v1[p1]); p1++; }
    		else {tmp.push_back(min(v1[p1],v2[p2])); p1++; p2++; }
    		
    		if (tmp.size() == k) break;
    	}
    	
    	p1 = p2 = 0;
    	vector<pair<int,int> > final2;
    	while(p1 < final.size() || p2 < tmp.size())
    	{
    		if (p1 == final.size() || (p2!=tmp.size()&&tmp[p2].first < final[p1].first)) {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p2++;}
    		else if (p2 == tmp.size() || final[p1].first < tmp[p2].first) {final2.push_back(final[p1]); p1++; }
    		else if (final[p1].second <= tmp[p2].second) {final2.push_back(final[p1]); p1++; p2++;}
    		else {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p1++; p2++;}
	   		if (final2.size() == k) break;
    	}
    	final = final2;
    	
    	
    }
    void mergeIn(vector<pair<int,int> >& final, vector<pair<int,int> >& v1, vector<pair<int,int> >& v2, vector<pair<int,int> >& store, int k)
    {
    	//merge v1, v2, store in tmp, merge tmp, final store in final, keep change in store
    	vector<pair<int,int> > tmp;
    	int p1,p2;
    	p1 = p2 = 0;
    	while(p1 < v1.size() || p2 < v2.size())
    	{
    		if (p1 == v1.size() || (p2!=v2.size()&&v2[p2].first < v1[p1].first)) {tmp.push_back(v2[p2]); p2++; }
    		else if (p2 == v2.size() || v1[p1].first < v2[p2].first) {tmp.push_back(v1[p1]); p1++; }
    		else {tmp.push_back(max(v1[p1],v2[p2])); p1++; p2++; } //max
    		
    		if (tmp.size() == k) break;
    	}
    	p1 = p2 = 0;
    	vector<pair<int,int> > final2;
    	while(p1 < final.size() || p2 < tmp.size())
    	{
    		if (p1 == final.size() || (p2!=tmp.size() && tmp[p2].first < final[p1].first)) {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p2++;}
    		else if (p2 == tmp.size() || final[p1].first < tmp[p2].first) {final2.push_back(final[p1]); p1++; }
    		else if (final[p1].second >= tmp[p2].second) {final2.push_back(final[p1]); p1++; p2++;} //>=
    		else {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p1++; p2++;}
    		if (final2.size() == k) break;
    	}
    	final = final2;
    }
    void printTransformInfo()
    {
    	//print
		/*
		cout << id << endl;
		cout << "in" << endl;
		for (int i = 0; i < Vin.size(); ++ i)
		{
			cout << "t:" << Vin[i] << " ";
			for (int j = 0; j < VinNeighbors[i].size(); ++ j) cout << VinNeighbors[i][j].v << " " << VinNeighbors[i][j].w << " ";
			cout << "| "; 
		}
		cout << endl;
		
		cout << "out" << endl;
		for (int i = 0; i < Vout.size(); ++ i)
		{
			cout << "t:" << Vout[i] << " ";
			for (int j = 0; j < VoutNeighbors[i].size(); ++ j) cout << VoutNeighbors[i][j].v << " " << VinNeighbors[i][j].w << " ";
			cout << "| ";
		}
		cout << endl;
		*/
		
		int tmp;
		for (int i = 0; i < Vin.size(); ++ i)
		{
			tmp = Vin[i];
			for (int j = 0; j < VinNeighbors[i].size(); ++ j) {tmp = VinNeighbors[i][j].v; tmp = VinNeighbors[i][j].w;}
			
		}
		
		for (int i = 0; i < Vout.size(); ++ i)
		{
			tmp = Vout[i];
			for (int j = 0; j < VoutNeighbors[i].size(); ++ j) {tmp = VoutNeighbors[i][j].v; tmp = VinNeighbors[i][j].w;}
		}
    }
    virtual void preCompute(MessageContainer& messages, int phaseNum)
    {
    	if (phaseNum == 1)
    	{
			if (superstep() == 1) //build neighbors
			{
				//build out-neighbors
				countOut = 0;
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
				{
					for (int j = 0; j < nqvalue().tw[i].size(); ++ j)
					{
						if (mOut.count(nqvalue().tw[i][j].first) == 0) 
						{
							mOut[nqvalue().tw[i][j].first] = 1;
						}
					}
				}
				for (std::map<int,int>::iterator it = mOut.begin(); it != mOut.end(); it ++)
				{
					it->second = countOut ++;
					Vout.push_back(it->first);
				}
				assert(countOut == Vout.size());
				VoutNeighbors.resize(countOut);
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
				{
					for (int j = 0; j < nqvalue().tw[i].size(); ++ j)
					{
						VoutNeighbors[mOut[nqvalue().tw[i][j].first] ].push_back(vw{nqvalue().neighbors[i], nqvalue().tw[i][j].second});		
					}
				}
				//out-neighbors built done
				
				
				
				//send messages
				//message: id, t+w, w
				vector<int> send(3);
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
				{
					//send_message(nqvalue().neighbors[i], id);
					send[0]=id;
					for (int j = 0; j < nqvalue().tw[i].size(); ++ j)
					{
						send[1]=nqvalue().tw[i][j].first+nqvalue().tw[i][j].second;
						send[2]=nqvalue().tw[i][j].second;
						send_message(nqvalue().neighbors[i], send);
					}
				}
				//send done
				
				//release memory
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i) vector<pair<int,int> >().swap(nqvalue().tw[i]);
				vector<int>().swap(nqvalue().neighbors);
				vector<vector<pair<int,int> > >().swap(nqvalue().tw);
			}
			else if (superstep() == 2)
			{
				
				//build in-neighbors
				countIn = 0;
				for (int i = 0; i < messages.size(); ++ i)
				{
					vector<int>& edge = messages[i];
					if (mIn.count(edge[1]) == 0) mIn[edge[1]] = 1;
				}
				for (std::map<int,int>::iterator it = mIn.begin();it!=mIn.end();it++)
				{
					it->second = countIn ++;
					Vin.push_back(it->first);
				}
				assert(countIn == Vin.size());
				VinNeighbors.resize(countIn);
				for (int i = 0; i < messages.size(); ++ i)
				{
					vector<int>& edge = messages[i];
					VinNeighbors[mIn[edge[1]] ].push_back(vw{edge[0],edge[2]});
				}
				//in-neighbors built done
				
				
				//build toIn, toOut
				toIn.resize(Vout.size(),-1);
				toOut.resize(Vin.size(),-1);
				
				int lastOut = -1;
				for (int i = Vin.size()-1; i >= 0; --i)
				{
					std::map<int,int>::iterator it;
					it = mOut.lower_bound(Vin[i]);
					if (it!=mOut.end() && it->second != lastOut)
					{
						lastOut = it->second;
						toOut[i] = it->second;
						toIn[it->second] = i;
					}
				}
				
				//cout << "id:" << id << endl;
				//initialize inDegree 
				inDegreeIn.resize(Vin.size());
				inDegreeOut.resize(Vout.size());
				for (int i = 0; i < Vin.size(); ++ i)
				{
					inDegreeIn[i] = VinNeighbors[i].size()+(i==0?0:1);
					//cout << id << " " << Vin[i] << " " << inDegreeIn[i] << endl;
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					inDegreeOut[i] = (toIn[i]==-1?0:1) + (i==0?0:1);
					//cout << id << " " << Vout[i] << " " << inDegreeOut[i] << endl;
				}
				
				//build neighbors done
				//printTransformInfo();
				
				
				vote_to_halt();
			}
			
		}
		else if (phaseNum == 2) //build topological level
		{
			//cout << "phaseNum: " << phaseNum << endl;
			if (superstep() == 1)
			{
				
				countinDegreeIn.resize(Vin.size(), 0);
				countinDegreeOut.resize(Vout.size(), 0);
				topologicalLevelIn.resize(Vin.size(), -1);
				topologicalLevelOut.resize(Vout.size(), -1);
				
				vector<int> send(2); //message: level, arrivalTime, 
				if (Vout.size() > 0 && inDegreeOut[0] == 0)
				{
					topologicalLevelOut[0] = 0;
					send[0] = 1;
					for (int j = 0; j < VoutNeighbors[0].size(); ++ j)
					{
						send[1] = Vout[0] + VoutNeighbors[0][j].w;
						send_message(VoutNeighbors[0][j].v, send);
					}
					
					//check other Vout
					int nxt_id = 1;
					while(nxt_id < Vout.size())
					{
						countinDegreeOut[nxt_id] ++;
						topologicalLevelOut[nxt_id] = topologicalLevelOut[nxt_id-1] + 1;
						if (countinDegreeOut[nxt_id] == inDegreeOut[nxt_id])
						{
							send[0] = topologicalLevelOut[nxt_id]+1;
							for (int j = 0; j < VoutNeighbors[nxt_id].size(); ++ j)
							{
								send[1] = Vout[nxt_id] + VoutNeighbors[nxt_id][j].w;
								send_message(VoutNeighbors[nxt_id][j].v, send);
							}
							nxt_id++;
						}
						else
						{
							break;
						}
					}
				}
			}
			else
			{
				queue<pair<int,bool> > q; //id, 0/1,    0 for Vin, 1 for Vout
				for (int i = 0; i < messages.size(); ++ i)
				{
					vector<int>& inMsg = messages[i];
					int in_index = mIn[inMsg[1]];
					countinDegreeIn[in_index] ++;
					
					topologicalLevelIn[in_index] = max(topologicalLevelIn[in_index],inMsg[0]);
					if (countinDegreeIn[in_index] == inDegreeIn[in_index])
					{
						q.push(make_pair(in_index, 0));
					}
				}
				while(!q.empty())
				{
					pair<int,bool> cur = q.front(); q.pop();
					if (cur.second == 0) //Vin
					{
						if (cur.first != Vin.size()-1)
						{
							int tmp = cur.first+1;
							countinDegreeIn[tmp] ++;
							topologicalLevelIn[tmp] = max(topologicalLevelIn[tmp],topologicalLevelIn[cur.first]+1);
							if (countinDegreeIn[tmp] == inDegreeIn[tmp])
							{
								q.push(make_pair(tmp, 0));
							}
						}
						int tmp = toOut[cur.first];
						if (tmp != -1)
						{
							countinDegreeOut[tmp] ++;
							topologicalLevelOut[tmp] = max(topologicalLevelOut[tmp],topologicalLevelIn[cur.first]+1);
							if (countinDegreeOut[tmp] == inDegreeOut[tmp])
							{
								q.push(make_pair(tmp,1));
							}
						}
					}
					else if (cur.second == 1) //Vout
					{
						if (cur.first != Vout.size()-1)
						{
							int tmp = cur.first+1;
							countinDegreeOut[tmp]++;
							topologicalLevelOut[tmp] = max(topologicalLevelOut[tmp],topologicalLevelOut[cur.first]+1);
							if (countinDegreeOut[tmp] == inDegreeOut[tmp])
							{
								q.push(make_pair(tmp,1));
							}
						}
						vector<int> send(2); //level, arrivalTime
						send[0] = topologicalLevelOut[cur.first]+1;
						for (int i = 0; i < VoutNeighbors[cur.first].size(); ++ i)
						{
							send[1] = Vout[cur.first]+VoutNeighbors[cur.first][i].w;
							send_message(VoutNeighbors[cur.first][i].v, send);
						}
					}
				}
			}
			vote_to_halt();
		}
		else if (phaseNum == 3) //build topChain
		{
			
			if (superstep() == 1)
			{
				//release memory
				vector<int>().swap(countinDegreeIn);
				vector<int>().swap(countinDegreeOut);
				vector<int>().swap(inDegreeIn);
				vector<int>().swap(inDegreeOut);
			
				LinIn.resize(Vin.size()); //inlabel for Vin
				LoutIn.resize(Vin.size()); //outlabel for Vin
				LinOut.resize(Vout.size());
				LoutOut.resize(Vout.size());
				
				LtinIn.resize(Vin.size());
				//LtoutIn.resize(Vin.size());
				//LtinOut.resize(Vout.size());
				LtoutOut.resize(Vout.size());
				
				LcinIn.resize(Vin.size());
				LcoutIn.resize(Vin.size());
				LcinOut.resize(Vout.size());
				LcoutOut.resize(Vout.size());
				
				
				vector<int> send(3);
				for (int i = 0; i < Vin.size(); ++ i)
				{
					LinIn[i].push_back(make_pair(id, Vin[i]));
					LoutIn[i].push_back(make_pair(id, Vin[i]));
					
					send[1] = LoutIn[i][0].first;
					send[2] = LoutIn[i][0].second;
					for (int j = 0; j < VinNeighbors[i].size(); ++ j)
					{
						send[0] = Vin[i]-VinNeighbors[i][j].w;	//time
						send_message(VinNeighbors[i][j].v, send);
					}
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					LinOut[i].push_back(make_pair(id, Vout[i]));
					LoutOut[i].push_back(make_pair(id, Vout[i]));
					
					send[1] = LinOut[i][0].first;
					send[2] = LinOut[i][0].second;
					for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
					{
						send[0] = -(Vout[i]+VoutNeighbors[i][j].w); //- means in-label
						send_message(VoutNeighbors[i][j].v, send);
					}
				}
				
			}
			else //message: t(in-, out+), <int,int>
			{
				//clear Lt, Lc
				for (int i = 0; i < Vin.size(); ++ i)
				{
					LtinIn[i].clear();
					//LtoutIn[i].clear();
					LcinIn[i].clear();
					LcoutIn[i].clear();
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					//LtinOut[i].clear();
					LtoutOut[i].clear();
					LcinOut[i].clear();
					LcoutOut[i].clear();
				}
				for (int i = 0; i < messages.size(); ++ i) //receive messages and save in Lt
				{
					vector<int>& msg = messages[i];
					
					if (msg[0] >= 0) //out-label
					{
						LtoutOut[mOut[msg[0]]].push_back(make_pair(msg[1], msg[2]));
					}
					else if (msg[0] < 0) //in-label
					{
						LtinIn[mIn[-msg[0]]].push_back(make_pair(msg[1], msg[2]));
					}
				}
				vector<pair<int,int> >::iterator it;
				for (int i = 0; i < Vin.size(); ++ i) 
				{
					sort(LtinIn[i].begin(), LtinIn[i].end(), cmp2);
					
					for (int j = 1; j < LtinIn[i].size(); ++ j)
					{
						if (LtinIn[i][j].first == LtinIn[i][j-1].first) LtinIn[i][j] = LtinIn[i][j-1];
					}
					it = unique(LtinIn[i].begin(), LtinIn[i].end() );
					LtinIn[i].resize(min((int)std::distance(LtinIn[i].begin(),it),labelSize) );
					
				}
				for (int i = 0; i < Vout.size(); ++ i) 
				{
					sort(LtoutOut[i].begin(), LtoutOut[i].end());
					
					for (int j = 1; j < LtoutOut[i].size(); ++ j)
					{
						if (LtoutOut[i][j].first == LtoutOut[i][j-1].first) LtoutOut[i][j] = LtoutOut[i][j-1];
					}
					it = unique(LtoutOut[i].begin(), LtoutOut[i].end());
					LtoutOut[i].resize(min((int)std::distance(LtoutOut[i].begin(),it),labelSize) );
					
				}
				
				//reverse topological order
				int p1 = Vin.size()-1;
				int p2 = Vout.size()-1;
				while(p1>=0||p2>=0)
				{
					if (p1 < 0 || (p2>=0&&Vin[p1] <= Vout[p2]))
					{
						//visit Vout[p2], then p2--
						//merge LoutOut[p2] with LtoutOut[p2] and potentially LcoutOut[p2+1]
						if (p2 == Vout.size()-1)
						{
							vector<pair<int,int> > tmp;
							mergeOut(LoutOut[p2], tmp, LtoutOut[p2], LcoutOut[p2], labelSize);
						}
						else mergeOut(LoutOut[p2], LcoutOut[p2+1], LtoutOut[p2], LcoutOut[p2], labelSize);
						
						p2--;
					}
					else if (p2 < 0 || Vin[p1] > Vout[p2])
					{
						//visit Vin[p1], then p1 --
						if (p1 == Vin.size()-1)
						{
							vector<pair<int,int> > tmp;//,tmp2;
							if (toOut[p1] == -1) ;//mergeOut(LoutIn[p1], tmp, tmp2, LcoutIn[p1], labelSize);
							else mergeOut(LoutIn[p1], tmp, LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
						}
						else
						{
							if (toOut[p1] == -1 )
							{
								vector<pair<int,int> > tmp;
								mergeOut(LoutIn[p1], LcoutIn[p1+1], tmp, LcoutIn[p1], labelSize);
							}
							else mergeOut(LoutIn[p1], LcoutIn[p1+1], LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
						}
						p1--;
					}
				}
				//topological order
				p1 = p2 = 0;
				while(p1 < Vin.size() || p2 < Vout.size())
				{
					if (p1 == Vin.size() || (p2!=Vout.size()&&Vin[p1] > Vout[p2]))
					{
						//visit Vout[p2]
						if (p2 == 0)
						{
							vector<pair<int,int> > tmp;//,tmp2;
							if (toIn[p2] == -1) ; //mergeIn(LinOut[p2], tmp, tmp2, LcinOut[p2], labelSize);
							else mergeIn(LinOut[p2], tmp, LcinIn[toIn[p2]], LcinOut[p2], labelSize);
						}
						else 
						{
							if (toIn[p2] == -1) 
							{
								vector<pair<int,int> > tmp;
								mergeIn(LinOut[p2], LcinOut[p2-1], tmp, LcinOut[p2], labelSize);
							}
							else mergeIn(LinOut[p2], LcinOut[p2-1], LcinIn[toIn[p2]], LcinOut[p2], labelSize);
						}
						p2++;
					}
					else if (p2 == Vout.size() || Vin[p1] <= Vout[p2])
					{
						if (p1 == 0)
						{	
							vector<pair<int,int> > tmp;
							mergeIn(LinIn[p1], tmp, LtinIn[p1], LcinIn[p1], labelSize);
						}
						else mergeIn(LinIn[p1], LcinIn[p1-1], LtinIn[p1], LcinIn[p1], labelSize);
						p1++;
					}
				}
				//send messages
				vector<int> send(3);
				for (int i = 0; i < Vin.size(); ++ i)
				{
					if (LcoutIn[i].size() > 0)
					{
						for (int j = 0; j < VinNeighbors[i].size(); ++ j)
						{
							send[0] = Vin[i]-VinNeighbors[i][j].w;
							for (int k = 0; k < LcoutIn[i].size(); ++ k)
							{
								send[1] = LcoutIn[i][k].first;
								send[2] = LcoutIn[i][k].second;
								send_message(VinNeighbors[i][j].v, send);
							}
						}
					}
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					if (LcinOut.size() > 0)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = -(Vout[i]+VoutNeighbors[i][j].w);
							for (int k = 0; k < LcinOut[i].size(); ++ k)
							{
								send[1] = LcinOut[i][k].first;
								send[2] = LcinOut[i][k].second;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		else if (phaseNum == 4)
		{
			if (superstep() == 1)
			{
				for (int i = 0; i < Vin.size(); ++ i)
				{
					LtinIn[i].clear();
					//LtoutIn[i].clear();
					LcinIn[i].clear();
					LcoutIn[i].clear();
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					//LtinOut[i].clear();
					LtoutOut[i].clear();
					LcinOut[i].clear();
					LcoutOut[i].clear();
				}
				/*
				cout << "id: " << id << endl;
				cout << "Vin" << endl;
				for (int i = 0; i < Vin.size(); ++ i)
				{
					cout << Vin[i] << endl;
					cout << "Lin: ";
					for (int j = 0; j < LinIn[i].size(); ++ j) cout << LinIn[i][j].first<< " " << LinIn[i][j].second << "; ";
					cout << endl;
					cout << "Lout: ";
					for (int j = 0; j < LoutIn[i].size(); ++ j) cout << LoutIn[i][j].first<< " " << LoutIn[i][j].second << "; ";
					cout << endl;
				}
				cout << "Vout" << endl;
				for (int i = 0; i < Vout.size(); ++ i)
				{
					cout << Vout[i] << endl;
					cout << "Lin: ";
					for (int j = 0; j < LinOut[i].size(); ++ j) cout << LinOut[i][j].first<< " " << LinOut[i][j].second << "; ";
					cout << endl;
					cout << "Lout ";
					for (int j = 0; j < LoutOut[i].size(); ++ j) cout << LoutOut[i][j].first<< " " << LoutOut[i][j].second << "; ";
					cout << endl;
				}
				*/
				
			}		
		}
    }
    
    
};

void temporalVertex::compute(MessageContainer& messages)
{
	vector<int>& queryContainer = *get_query();
	//cout << "queryContainer: " << endl;
	//for (int i = 0; i < queryContainer.size(); ++ i) cout << queryContainer[i] << endl;
	
	//get neighbors: 0/1, v, (t1,t2)
	if (queryContainer[0] == OUT_NEIGHBORS_QUERY)
	{
		/*
		if (superstep() == 1)
		{
			int t1 = 0; int t2 = inf;
			if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			for (it = it1; it!=mOut.end() && it!=it2; ++it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					cout << "(" << VoutNeighbors[idx][j].v << " " << Vout[idx] << " " << VoutNeighbors[idx][j].w << ")"<< endl;
				}
			}
		}
		*/
		vote_to_halt();
	}
	else if (queryContainer[0] == IN_NEIGHBORS_QUERY)
	{
		/*
		if (superstep() == 1)
		{
			int t1 = 0; int t2 = inf;
			if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
			std::map<int,int>::iterator it,it1,it2;
			it1 = mIn.lower_bound(t1);
			it2 = mIn.upper_bound(t2);
			int idx;
			for (it = it1; it!=mIn.end() && it!=it2; ++it)
			{
				idx = it->second;
				for (int j = 0; j < VinNeighbors[idx].size(); ++ j)
				{
					cout << "(" << VinNeighbors[idx][j].v << " " << Vin[idx] << " " << VinNeighbors[idx][j].w << ")"<< endl;
				}
			}
		}
		*/
		vote_to_halt();
	}
	else if (queryContainer[0] == REACHABILITY_QUERY)
	{
		/*
			message type: t;
		*/
		int t1 = 0;
		int t2 = inf;
		if (superstep() == 1)
		{
			if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
			if (id == queryContainer[2])
			{
				//done
				cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
				canVisit();
				forceTerminate();  
				qvalue()[0] = -1; //can visit label 
				return;
			}
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it!=it2; ++it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t1;
		}
		else
		{
			if (queryContainer.size() == 5) {t2 = queryContainer[4];}		
			int& lastT = qvalue()[0];
			//receive messages
			int mini = inf;
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			if (id == queryContainer[2] && mini <= t2)
			{
				//done
				cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
				canVisit();
				forceTerminate(); 
				qvalue()[0] = -1; //can visit label
				return;
			}
			if (mini < lastT)
			{	
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				if (it != mOut.end())
				{
					start = it->second;
					end = min(lastT, t2);
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = Vout[i] + VoutNeighbors[i][j].w;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				//reset lastT
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == REACHABILITY_QUERY_TOPCHAIN)
	{
		/*
			message type: t;
		*/
		int t1 = 0;
		int t2 = inf;
		if (superstep() == 1)
		{
			TaskT& task=*(TaskT*)query_entry();
			if (task.dst_info[0] == -1){
				vote_to_halt(); return;
			}
			
			if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
			if (id == queryContainer[2])
			{
				//done
				cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
				canVisit();
				forceTerminate(); 
				qvalue()[0] = -1; //can visit label
				return;
			}
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it!=it2; ++it)
			{
				idx = it->second;
				int ret = labelCompare(idx);
				if (ret != 0)
				{
					if (ret == 1) //can visit
					{
						send[0] = 0;
						send_message(queryContainer[2], send);
						break;
					}
					else if (ret == -1) //cannot visit from this vertex, need to be pruned
					{
						break;
					}
				}
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t1;
		}
		else
		{
			if (queryContainer.size() == 5) {t2 = queryContainer[4];}		
			int& lastT = qvalue()[0];
			//receive messages
			int mini = inf;
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			if (id == queryContainer[2] && mini <= t2)
			{
				//done
				cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
				canVisit();
				forceTerminate(); 
				qvalue()[0] = -1; //can visit label
				return;
			}
			if (mini < lastT)
			{	
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				if (it != mOut.end()) 
				{
					start = it->second;
					end = min(lastT, t2);
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
					{
						int ret = labelCompare(i);
						if (ret != 0)
						{
							if (ret == 1) //can visit
							{
								send[0] = 0;
								send_message(queryContainer[2], send);
								break;
							}
							else if (ret == -1) //cannot visit from this vertex, need to be pruned
							{
								break;
							}
						}
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = Vout[i] + VoutNeighbors[i][j].w;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				//reset lastT
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == EARLIEST_QUERY_TOPCHAIN)
	{
		/*
			message type: t;
		*/
		
		int t1 = 0;
		int t2 = inf;
		if (superstep() == 1 || restart() )
		{
			TaskT& task=*(TaskT*)query_entry();
			if (task.dst_info[0] == -1){
				vote_to_halt(); return;
			}
			//cout << "starting vertex: " << id << endl;
			if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
			if (id == queryContainer[2])
			{
				//done
				//cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
				canVisit();
				forceTerminate(); 
				return;
			}
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it!=it2; ++it)
			{
				idx = it->second;
				
				int ret = labelCompare(idx);
				if (ret != 0)
				{
					if (ret == 1) //can visit
					{
						send[0] = 0;
						send_message(queryContainer[2], send);
						break;
					}
					else if (ret == -1) //cannot visit from this vertex, need to be pruned
					{
						break;
					}
				}
				
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					//cout << "(" << VoutNeighbors[idx][j].v << " " << Vout[idx] << " " << VoutNeighbors[idx][j].w << ")"<< endl;
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t1;
		}
		else
		{
			if (queryContainer.size() == 5) {t2 = queryContainer[4];}		
			int& lastT = qvalue()[0];
			//receive messages
			int mini = inf;
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			if (id == queryContainer[2] /*mini <= t2*/)
			{
				//done
				TaskT& task=*(TaskT*)query_entry();
				if (mini <= Vin[task.m])
				{
					//cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
					canVisit();
					forceTerminate(); return;
				}
				else {vote_to_halt(); return;}
			}
			if (mini < lastT)
			{	
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				if (it != mOut.end())
				{
					start = it->second;
					end = min(lastT, t2);
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
					{
						int ret = labelCompare(i);
						if (ret != 0)
						{
							if (ret == 1) //can visit
							{
								send[0] = 0;
								send_message(queryContainer[2], send);
								break;
							}
							else if (ret == -1) //cannot visit from this vertex, need to be pruned
							{
								break;
							}
						}
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = Vout[i] + VoutNeighbors[i][j].w;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				//reset lastT
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == FASTEST_QUERY_TOPCHAIN)
	{
		/*
			message type: t;
		*/
		
		int t1 = 0;
		int t2 = inf;
		if (superstep() == 1 || restart() )
		{
			//cout << "starting vertex: " << id << endl;
			if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
			if (id == queryContainer[2])
			{
				//done
				cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
				canVisit();
				forceTerminate(); 
				return;
			}
			
			int idx;
			idx = getrsrc(); //get start vertex
			if (idx < 0) {vote_to_halt(); return;}
			
			vector<int> send(1);
			
			int ret = labelCompare(idx);
			if (ret != 0)
			{
				if (ret == 1) //can visit
				{
					send[0] = 0;
					send_message(queryContainer[2], send);
					return;
				}
				else if (ret == -1) //cannot visit from this vertex, need to be pruned
				{
					vote_to_halt();
					return;
				}
			}
			
			for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
			{
				//cout << "(" << VoutNeighbors[idx][j].v << " " << Vout[idx] << " " << VoutNeighbors[idx][j].w << ")"<< endl;
				send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
				send_message(VoutNeighbors[idx][j].v, send);
			}
		}
		else
		{
			if (queryContainer.size() == 5) {t2 = queryContainer[4];}		
			int& lastT = qvalue()[0];
			//receive messages
			int mini = inf;
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			//cout << "id:" << id << " mini:" << mini << " lastT:" << lastT << endl;
			if (id == queryContainer[2] /*mini <= t2*/)
			{
				//done
				TaskT& task=*(TaskT*)query_entry();
				if (mini <= Vin[task.m])
				{
					cout << queryContainer[1] << " can visit " << queryContainer[2] << endl;
					canVisit();
					forceTerminate(); return;
				}
				else {vote_to_halt(); return;}
			}
			if (mini < lastT)
			{	
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				start = it->second;
				end = min(lastT, t2);
				vector<int> send(1);
				for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
				{
					
					int ret = labelCompare(i);
					if (ret != 0)
					{
						if (ret == 1) //can visit
						{
							send[0] = 0;
							send_message(queryContainer[2], send);
							return;
						}
						else if (ret == -1) //cannot visit from this vertex, need to be pruned
						{
							vote_to_halt();
							return;
						}
					}
					
					for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
					{
						send[0] = Vout[i] + VoutNeighbors[i][j].w;
						send_message(VoutNeighbors[i][j].v, send);
					}
				}
				//reset lastT
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == EARLIEST_QUERY) //analytic query
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t1;
		}
		else
		{
			int mini = inf;
			int& lastT = qvalue()[0];
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			if (mini < lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				if (it != mOut.end()) {
					start = it->second;
					end = min(lastT, t2);
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = Vout[i] + VoutNeighbors[i][j].w;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	
	else if (queryContainer[0] == LATEST_QUERY) //analytic query
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mIn.lower_bound(t1);
			it2 = mIn.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it != mIn.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VinNeighbors[idx].size(); ++ j)
				{
					send[0] = -(Vin[idx] - VinNeighbors[idx][j].w);
					send_message(VinNeighbors[idx][j].v, send);
				}
			}
		}
		else
		{
			int maxi = 0;
			int& lastT = qvalue()[0];
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (-messages[i][0] > maxi) maxi = -messages[i][0];
			}
			if (maxi > lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mIn.upper_bound(max(t1,lastT));
				if (it != mIn.end())
				{
					start = it->second;
					end = maxi;
					vector<int> send(1);
					for (int i = start; i < Vin.size() && Vin[i] <= end; ++ i)
					{
						for (int j = 0; j < VinNeighbors[i].size(); ++ j)
						{
							send[0] = -(Vin[i] - VinNeighbors[i][j].w);
							send_message(VinNeighbors[i][j].v, send);
						}
					}
				}
				lastT = maxi;
			}
		}
		vote_to_halt();
	}
	/*
	else if (queryContainer[0] == LATEST_QUERY) //analytic query for experiment
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = -(Vout[idx] - VoutNeighbors[idx][j].w);
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t2;
		}
		else
		{
			int maxi = 0;
			int& lastT = qvalue()[0];
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (-messages[i][0] > maxi) maxi = -messages[i][0];
			}
			if (maxi > lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.upper_bound(max(t1,lastT));
				if (it != mOut.end())
				{
					start = it->second;
					end = maxi;
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] <= end; ++ i)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = -(Vout[i] - VoutNeighbors[i][j].w);
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				lastT = maxi;
			}
		}
		vote_to_halt();
	}
	*/
	else if (queryContainer[0] == FASTEST_QUERY) //analytic query
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(2);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send[1] = Vout[idx];
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			for (int i = 0; i < Vin.size(); ++ i) latestArrivalTime[i] = Vin[i];
			qvalue()[0] = 0;
		}
		else
		{
			vector<int> changed;
			for (int i = 0; i < messages.size(); ++ i)
			{
				std::map<int,int>::iterator it;
				it = mIn.lower_bound(messages[i][0]);
				int idx = it->second;
				if (latestArrivalTime[idx] < messages[i][1])
				{
					latestArrivalTime[idx] = messages[i][1];
					changed.push_back(idx);
					if (Vin[idx] - latestArrivalTime[idx] < qvalue()[0]) qvalue()[0] = Vin[idx] - latestArrivalTime[idx]; //store the fastest
				} 
			}
			std::sort(changed.begin(), changed.end());
			std::vector<int>::iterator it;
			it = std::unique(changed.begin(), changed.end());
			changed.resize(std::distance(changed.begin(),it));
			if (changed.size() > 0)
			{
				int currMax = latestArrivalTime[changed[0]];
				for (int i = 1; i < changed.size(); ++ i)
				{
					currMax = max(currMax, latestArrivalTime[changed[i]]);
					latestArrivalTime[changed[i]] = currMax;
				}
				std::map<int,int>::iterator itOut = mOut.lower_bound(Vin[changed[0]]);
				if (itOut != mOut.end())
				{
					int start = itOut->second;
					int shouldSend = latestArrivalTime[changed[0]];
					int pt = 0;
					vector<int> send(2);
					for (int i = start; i < Vout.size() && Vout[i] < t2; ++ i)
					{
						while(pt < changed.size() && Vin[changed[pt]] <= Vout[i]) 
						{
							shouldSend = max(shouldSend, latestArrivalTime[changed[pt]]);
							pt++;
						}
						if (shouldSend > lastOut[i])
						{
							lastOut[i] = shouldSend;
							send[1] = shouldSend;
							for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
							{
								send[0] = Vout[i] + VoutNeighbors[i][j].w;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == SHORTEST_QUERY) //analytic query
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 4) {t1 = queryContainer[2]; t2 = queryContainer[3];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(2);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send[1] = VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			for (int i = 0; i < Vin.size(); ++ i) latestArrivalTime[i] = 0;
			qvalue()[0] = 0;
		}
		else
		{
			vector<int> changed;
			for (int i = 0; i < messages.size(); ++ i)
			{
				std::map<int,int>::iterator it;
				it = mIn.lower_bound(messages[i][0]);
				int idx = it->second;
				if (messages[i][1] < latestArrivalTime[idx])
				{
					latestArrivalTime[idx] = messages[i][1];
					changed.push_back(idx); 
					if (latestArrivalTime[idx] < qvalue()[0]) qvalue()[0] = latestArrivalTime[idx]; //store the shortest
				} 
			}
			std::sort(changed.begin(), changed.end());
			std::vector<int>::iterator it;
			it = std::unique(changed.begin(), changed.end());
			changed.resize(std::distance(changed.begin(),it));
			if (changed.size() > 0)
			{
				int currMin = latestArrivalTime[changed[0]];
				for (int i = 1; i < changed.size(); ++ i)
				{
					currMin = min(currMin, latestArrivalTime[changed[i]]);
					latestArrivalTime[changed[i]] = currMin;
				}
				std::map<int,int>::iterator itOut = mOut.lower_bound(Vin[changed[0]]);
				if (itOut != mOut.end())
				{
					int start = itOut->second;
					int shouldSend = latestArrivalTime[changed[0]];
					int pt = 0;
					vector<int> send(2);
					for (int i = start; i < Vout.size() && Vout[i] < t2; ++ i)
					{
						while(pt < changed.size() && Vin[changed[pt]] <= Vout[i]) 
						{
							shouldSend = min(shouldSend, latestArrivalTime[changed[pt]]);
							pt++;
						}
						if (shouldSend < lastOut[i])
						{
							lastOut[i] = shouldSend;
							for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
							{
								send[0] = Vout[i] + VoutNeighbors[i][j].w;
								send[1] = shouldSend + VoutNeighbors[i][j].w;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == ADDEDGE) // 11, u, v, t, w
	{
		if (superstep() == 1)
		{
			if (id == queryContainer[1]) //src
			{
				if (Vout.size() == 0 || Vout[Vout.size()-1] < queryContainer[3]) //new Vout
				{
					int cur = Vout.size();
					
					Vout.push_back(queryContainer[3]);
					VoutNeighbors.resize(Vout.size());
					VoutNeighbors[cur].push_back(vw{queryContainer[2], queryContainer[4]});
					LinOut.resize(Vout.size());
					LoutOut.resize(Vout.size());
					LcinOut.resize(Vout.size());
					LcoutOut.resize(Vout.size());
					LtoutOut.resize(Vout.size());
					topologicalLevelOut.resize(Vout.size());
					toIn.resize(Vout.size());
					toIn[cur] = -1;
					mOut[queryContainer[3]] = cur;
					LoutOut[cur].push_back(make_pair(id, queryContainer[3]));
					LinOut[cur].push_back(make_pair(id, queryContainer[3]));

					std::map<int,int>::iterator it = mIn.upper_bound(queryContainer[3]);
					if (it != mIn.begin())
					{
						it--;
						int p = it->second;
						if (toOut[p] == -1) {toOut[p] = cur; toIn[cur] = p;}
					}
					if (toIn[cur] == -1)
					{
						if (cur == 0)
						{
							topologicalLevelOut[cur] = 0;
							;
						}
						else
						{
							topologicalLevelOut[cur] = topologicalLevelOut[cur-1]+1;
							vector<pair<int,int> > tmp, tmp2;
							mergeIn(LinOut[cur], LinOut[cur-1], tmp, tmp2, labelSize);
						}
					}
					else
					{
						if (cur == 0)
						{
							topologicalLevelOut[cur] = topologicalLevelIn[toIn[cur]]+1;
							vector<pair<int,int> > tmp, tmp2;
							mergeIn(LinOut[cur], LinIn[toIn[cur]], tmp, tmp2, labelSize);
						}
						else
						{
							vector<pair<int,int> > tmp;
							topologicalLevelOut[cur] = max(topologicalLevelIn[toIn[cur]],topologicalLevelOut[cur-1])+1;
							mergeIn(LinOut[cur], LinOut[cur-1], LinIn[toIn[cur]], tmp ,labelSize);
						}
					}
					/*
					cout << "topo" << endl;
					cout << topologicalLevelOut[cur] << endl;
					cout << "inLabel" << endl;
					for (int i = 0; i < LinOut[cur].size(); ++ i) cout << LinOut[cur][i].first << " " << LinOut[cur][i].second << endl;
					*/
				}
				else if (Vout[Vout.size()-1] == queryContainer[3])
				{
					VoutNeighbors[Vout.size()-1].push_back(vw{queryContainer[2], queryContainer[4]});
				}
				else assert(0);
				
				//send in-label to dst
				int cur = Vout.size()-1;
				vector<int> send;
				//send.push_back(LinOut[cur].size());
				for (int i = 0; i < LinOut[cur].size(); ++ i) {send.push_back(LinOut[cur][i].first); send.push_back(LinOut[cur][i].second);}
				send.push_back(topologicalLevelOut[cur]);
				send_message(queryContainer[2], send);
			}
			if (id == queryContainer[2])
			{
				int arrivalT = queryContainer[3]+queryContainer[4];
				if (Vin.size() == 0 || Vin[Vin.size()-1] < queryContainer[3]+queryContainer[4])
				{
					int cur = Vin.size();
					Vin.push_back(arrivalT);
					VinNeighbors.resize(Vin.size());
					VinNeighbors[cur].push_back(vw{queryContainer[1], queryContainer[4]});
					LinIn.resize(Vin.size());
					LoutIn.resize(Vin.size());
					LcinIn.resize(Vin.size());
					LcoutIn.resize(Vin.size());
					LtinIn.resize(Vin.size());
					topologicalLevelIn.resize(Vin.size());
					toOut.resize(Vin.size());
					toOut[cur] = -1;
					mIn[arrivalT] = cur;
					/*
					std::map<int,int>::iterator it = mOut.lower_bound(arrivalT);
					if (it != mOut.end())
					{
						int p = it->second;
						if (toIn[p] != -1)
						{
							int p2 = toIn[p];
							toOut[p2] = -1;
						}
						toIn[p] = cur;
						toOut[cur] = p;
					}
					if (toOut[cur]!=-1) LoutIn[cur] = LoutOut[toOut[cur]];
					else LoutIn[cur].push_back(make_pair(id, arrivalT));
					*/
					LoutIn[cur].push_back(make_pair(id, arrivalT));
					LinIn[cur].push_back(make_pair(id, arrivalT));
					topologicalLevelIn[cur] = cur==0?0:topologicalLevelIn[cur-1]+1;
					/*
					cout << "outLabel" << endl;
					for (int i = 0; i < LoutIn[cur].size(); ++ i) cout << LoutIn[cur][i].first << " " << LoutIn[cur][i].second << endl;
					*/
				}
				else if (Vin[Vin.size()-1] == arrivalT)
				{
					VinNeighbors[Vin.size()-1].push_back(vw{queryContainer[2], queryContainer[4]});
				}
				else assert(0);
				//send messages
				int cur = Vin.size()-1;
				vector<int> send;
				//send.push_back(LoutIn[cur].size());
				for (int i = 0; i < LoutIn[cur].size(); ++ i) {send.push_back(LoutIn[cur][i].first); send.push_back(LoutIn[cur][i].second);}
				send_message(queryContainer[1], send);
			}
		}
		else if (superstep() == 2)
		{
			if (id == queryContainer[1])
			{
				//clear Lt, Lc
				for (int i = 0; i < Vin.size(); ++ i)
				{
					//LtinIn[i].clear();
					//LtoutIn[i].clear();
					//LcinIn[i].clear();
					LcoutIn[i].clear();
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					//LtinOut[i].clear();
					LtoutOut[i].clear();
					//LcinOut[i].clear();
					LcoutOut[i].clear();
				}
				
				int cur = Vout.size()-1;
				for (int i = 0; i < messages[0].size(); i += 2)
				{
					LtoutOut[cur].push_back(make_pair(messages[0][i], messages[0][i+1]));
				}
				
				//reverse topological order
				int p1 = Vin.size()-1;
				int p2 = Vout.size()-1;
				while(p1>=0||p2>=0)
				{
					if (p1 < 0 || (p2>=0&&Vin[p1] <= Vout[p2]))
					{
						//visit Vout[p2], then p2--
						//merge LoutOut[p2] with LtoutOut[p2] and potentially LcoutOut[p2+1]
						if (p2 == Vout.size()-1)
						{
							vector<pair<int,int> > tmp;
							mergeOut(LoutOut[p2], tmp, LtoutOut[p2], LcoutOut[p2], labelSize);
						}
						else mergeOut(LoutOut[p2], LcoutOut[p2+1], LtoutOut[p2], LcoutOut[p2], labelSize);
						
						p2--;
					}
					else if (p2 < 0 || Vin[p1] > Vout[p2])
					{
						//visit Vin[p1], then p1 --
						if (p1 == Vin.size()-1)
						{
							vector<pair<int,int> > tmp;//,tmp2;
							if (toOut[p1] == -1) ;//mergeOut(LoutIn[p1], tmp, tmp2, LcoutIn[p1], labelSize);
							else mergeOut(LoutIn[p1], tmp, LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
						}
						else
						{
							if (toOut[p1] == -1 )
							{
								vector<pair<int,int> > tmp;
								mergeOut(LoutIn[p1], LcoutIn[p1+1], tmp, LcoutIn[p1], labelSize);
							}
							else mergeOut(LoutIn[p1], LcoutIn[p1+1], LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
						}
						p1--;
					}
				}
				
				//send messages
				vector<int> send(3);
				for (int i = 0; i < Vin.size(); ++ i)
				{
					if (LcoutIn[i].size() > 0)
					{
						for (int j = 0; j < VinNeighbors[i].size(); ++ j)
						{
							send[0] = Vin[i]-VinNeighbors[i][j].w;
							for (int k = 0; k < LcoutIn[i].size(); ++ k)
							{
								send[1] = LcoutIn[i][k].first;
								send[2] = LcoutIn[i][k].second;
								send_message(VinNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
			if (id == queryContainer[2])
			{
				int cur = Vin.size()-1;
				vector<pair<int,int> > tmp;
				for (int i = 0; i < messages[0].size()-1; i += 2)
				{
					tmp.push_back(make_pair(messages[0][i], messages[0][i+1]));
				}
				topologicalLevelIn[cur] = max(messages[0][messages[0].size()-1]+1, topologicalLevelIn[cur]);
				
				if (cur == 0) 
				{
					vector<pair<int,int> > tmp2,tmp3;
					mergeIn(LinIn[cur], tmp, tmp2, tmp3, labelSize);
				}
				else {
					vector<pair<int,int> > tmp2;
					mergeIn(LinIn[cur], tmp, LinIn[cur-1], tmp2, labelSize);
				}
			}
		}
		else
		{
			//clear Lt, Lc
			for (int i = 0; i < Vin.size(); ++ i)
			{
				//LtinIn[i].clear();
				//LtoutIn[i].clear();
				//LcinIn[i].clear();
				LcoutIn[i].clear();
			}
			for (int i = 0; i < Vout.size(); ++ i)
			{
				//LtinOut[i].clear();
				LtoutOut[i].clear();
				//LcinOut[i].clear();
				LcoutOut[i].clear();
			}
			for (int i = 0; i < messages.size(); ++ i) //receive messages and save in Lt
			{
				vector<int>& msg = messages[i];
				
				LtoutOut[mOut[msg[0]]].push_back(make_pair(msg[1], msg[2]));
			}
			vector<pair<int,int> >::iterator it;
			for (int i = 0; i < Vout.size(); ++ i) 
			{
				sort(LtoutOut[i].begin(), LtoutOut[i].end());
				for (int j = 1; j < LtoutOut[i].size(); ++ j)
				{
					if (LtoutOut[i][j].first == LtoutOut[i][j-1].first) LtoutOut[i][j] = LtoutOut[i][j-1];
				}
				it = unique(LtoutOut[i].begin(), LtoutOut[i].end());
				LtoutOut[i].resize(min((int)std::distance(LtoutOut[i].begin(),it),labelSize) );
			}
			
			//reverse topological order
			int p1 = Vin.size()-1;
			int p2 = Vout.size()-1;
			while(p1>=0||p2>=0)
			{
				if (p1 < 0 || (p2>=0&&Vin[p1] <= Vout[p2]))
				{
					//visit Vout[p2], then p2--
					//merge LoutOut[p2] with LtoutOut[p2] and potentially LcoutOut[p2+1]
					if (p2 == Vout.size()-1)
					{
						vector<pair<int,int> > tmp;
						mergeOut(LoutOut[p2], tmp, LtoutOut[p2], LcoutOut[p2], labelSize);
					}
					else mergeOut(LoutOut[p2], LcoutOut[p2+1], LtoutOut[p2], LcoutOut[p2], labelSize);
					
					p2--;
				}
				else if (p2 < 0 || Vin[p1] > Vout[p2])
				{
					//visit Vin[p1], then p1 --
					if (p1 == Vin.size()-1)
					{
						vector<pair<int,int> > tmp;//,tmp2;
						if (toOut[p1] == -1) ;//mergeOut(LoutIn[p1], tmp, tmp2, LcoutIn[p1], labelSize);
						else mergeOut(LoutIn[p1], tmp, LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
					}
					else
					{
						if (toOut[p1] == -1 )
						{
							vector<pair<int,int> > tmp;
							mergeOut(LoutIn[p1], LcoutIn[p1+1], tmp, LcoutIn[p1], labelSize);
						}
						else mergeOut(LoutIn[p1], LcoutIn[p1+1], LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
					}
					p1--;
				}
			}
			
			//send messages
			vector<int> send(3);
			for (int i = 0; i < Vin.size(); ++ i)
			{
				if (LcoutIn[i].size() > 0)
				{
					for (int j = 0; j < VinNeighbors[i].size(); ++ j)
					{
						send[0] = Vin[i]-VinNeighbors[i][j].w;
						for (int k = 0; k < LcoutIn[i].size(); ++ k)
						{
							send[1] = LcoutIn[i][k].first;
							send[2] = LcoutIn[i][k].second;
							send_message(VinNeighbors[i][j].v, send);
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == TOPKNEIGHBORS_EARLIEST) //top-k neighbors application; query: 22, src, type, t1, t2
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];} //different from EARLIEST_QUERY
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			int count1 = 0;
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				count1++;
				if (count1 > queryContainer[2]) break;
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t1;
		}
		else
		{
			int mini = inf;
			int& lastT = qvalue()[0];
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			
			//aggregator
			int& aggValue = *((int*)get_agg());
			if (mini > aggValue)
			{
				vote_to_halt();
				return;
			}
			t2 = min(t2, aggValue);
			
			if (mini < lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				if (it != mOut.end()) {
					start = it->second;
					end = min(lastT, t2);
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
					{
						if (i-start >= queryContainer[2]) break;
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = Vout[i] + VoutNeighbors[i][j].w;
							if (send[0] > aggValue) break;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == TOPKNEIGHBORS_LATEST) //top-k neighbors application; query: 24, src, type, t1, t2
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			int count1 = 0;
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				count1++;
				if (count1 > queryContainer[2]) break;
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = -(Vout[idx] - VoutNeighbors[idx][j].w);
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = -t2; //
		}
		else
		{
			int maxi = 0;
			int& lastT = qvalue()[0];
			lastT = -lastT; //
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (-messages[i][0] > maxi) maxi = -messages[i][0];
			}
			
			//aggregator
			int& aggValue = *((int*)get_agg());
			if (maxi < -aggValue)
			{
				vote_to_halt();
				return;
			}
			t1 = max(t1, -aggValue);
			
			if (maxi > lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.upper_bound(max(t1,lastT));
				if (it != mOut.end())
				{
					start = it->second;
					end = maxi;
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] <= end; ++ i)
					{
						if (i-start >= queryContainer[2]) break;
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = -(Vout[i] - VoutNeighbors[i][j].w);
							if (send[0] > aggValue) break;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				lastT = maxi;
			}
			lastT = -lastT; //
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == TOPKNEIGHBORS_FASTEST) //top-k neighbors application
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(2);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send[1] = Vout[idx];
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			for (int i = 0; i < Vin.size(); ++ i) latestArrivalTime[i] = Vin[i];
			qvalue()[0] = 0;
		}
		else
		{
			//aggregator
			int& aggValue = *((int*)get_agg());
			
			vector<int> changed;
			for (int i = 0; i < messages.size(); ++ i)
			{
				std::map<int,int>::iterator it;
				it = mIn.lower_bound(messages[i][0]);
				int idx = it->second;
				if (latestArrivalTime[idx] < messages[i][1])
				{
					latestArrivalTime[idx] = messages[i][1];
					changed.push_back(idx);
					if (Vin[idx] - latestArrivalTime[idx] < qvalue()[0]) qvalue()[0] = Vin[idx] - latestArrivalTime[idx]; //store the fastest
				} 
			}
			std::sort(changed.begin(), changed.end());
			std::vector<int>::iterator it;
			it = std::unique(changed.begin(), changed.end());
			changed.resize(std::distance(changed.begin(),it));
			if (changed.size() > 0)
			{
				int currMax = latestArrivalTime[changed[0]];
				for (int i = 1; i < changed.size(); ++ i)
				{
					currMax = max(currMax, latestArrivalTime[changed[i]]);
					latestArrivalTime[changed[i]] = currMax;
				}
				std::map<int,int>::iterator itOut = mOut.lower_bound(Vin[changed[0]]);
				if (itOut != mOut.end())
				{
					int start = itOut->second;
					int shouldSend = latestArrivalTime[changed[0]];
					int pt = 0;
					vector<int> send(2);
					for (int i = start; i < Vout.size() && Vout[i] < t2; ++ i)
					{
						while(pt < changed.size() && Vin[changed[pt]] <= Vout[i]) 
						{
							shouldSend = max(shouldSend, latestArrivalTime[changed[pt]]);
							pt++;
						}
						if (shouldSend > lastOut[i])
						{
							lastOut[i] = shouldSend;
							send[1] = shouldSend;
							for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
							{
								send[0] = Vout[i] + VoutNeighbors[i][j].w;
								if (send[0] - send[1] > aggValue) continue;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == TOPKNEIGHBORS_SHORTEST) //top-k neighbors application
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(2);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send[1] = VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			for (int i = 0; i < Vin.size(); ++ i) latestArrivalTime[i] = 0;
			qvalue()[0] = 0;
		}
		else
		{
			//aggregator
			int& aggValue = *((int*)get_agg());
			
			vector<int> changed;
			for (int i = 0; i < messages.size(); ++ i)
			{
				std::map<int,int>::iterator it;
				it = mIn.lower_bound(messages[i][0]);
				int idx = it->second;
				if (messages[i][1] < latestArrivalTime[idx])
				{
					latestArrivalTime[idx] = messages[i][1];
					changed.push_back(idx); 
					if (latestArrivalTime[idx] < qvalue()[0]) qvalue()[0] = latestArrivalTime[idx]; //store the shortest
				} 
			}
			std::sort(changed.begin(), changed.end());
			std::vector<int>::iterator it;
			it = std::unique(changed.begin(), changed.end());
			changed.resize(std::distance(changed.begin(),it));
			if (changed.size() > 0)
			{
				int currMin = latestArrivalTime[changed[0]];
				for (int i = 1; i < changed.size(); ++ i)
				{
					currMin = min(currMin, latestArrivalTime[changed[i]]);
					latestArrivalTime[changed[i]] = currMin;
				}
				std::map<int,int>::iterator itOut = mOut.lower_bound(Vin[changed[0]]);
				if (itOut != mOut.end())
				{
					int start = itOut->second;
					int shouldSend = latestArrivalTime[changed[0]];
					int pt = 0;
					vector<int> send(2);
					for (int i = start; i < Vout.size() && Vout[i] < t2; ++ i)
					{
						while(pt < changed.size() && Vin[changed[pt]] <= Vout[i]) 
						{
							shouldSend = min(shouldSend, latestArrivalTime[changed[pt]]);
							pt++;
						}
						if (shouldSend < lastOut[i])
						{
							lastOut[i] = shouldSend;
							for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
							{
								send[0] = Vout[i] + VoutNeighbors[i][j].w;
								send[1] = shouldSend + VoutNeighbors[i][j].w;
								if (send[1] > aggValue) continue;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == KHOP_EARLIEST) //KHOP: 26, src, k, t1, t2
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t1;
		}
		else
		{
			int mini = inf;
			int& lastT = qvalue()[0];
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] < mini) mini = messages[i][0];
			}
			//
			if (superstep() > queryContainer[2])
			{
				vote_to_halt();
				return;
			}
			
			if (mini < lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.lower_bound(mini);
				if (it != mOut.end()) {
					start = it->second;
					end = min(lastT, t2);
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] < end; ++ i)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = Vout[i] + VoutNeighbors[i][j].w;
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				lastT = mini;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == KHOP_LATEST) // k-hop application
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(1);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = -(Vout[idx] - VoutNeighbors[idx][j].w);
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			qvalue()[0] = t2; //
		}
		else
		{
			int maxi = 0;
			int& lastT = qvalue()[0];
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (-messages[i][0] > maxi) maxi = -messages[i][0];
			}
			//
			if (superstep() > queryContainer[2])
			{
				vote_to_halt();
				return;
			}
			
			if (maxi > lastT)
			{
				std::map<int,int>::iterator it;
				int start, end;
				it = mOut.upper_bound(max(t1,lastT));
				if (it != mOut.end())
				{
					start = it->second;
					end = maxi;
					vector<int> send(1);
					for (int i = start; i < Vout.size() && Vout[i] <= end; ++ i)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = -(Vout[i] - VoutNeighbors[i][j].w);
							send_message(VoutNeighbors[i][j].v, send);
						}
					}
				}
				lastT = maxi;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == KHOP_FASTEST) //k-hop application
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(2);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send[1] = Vout[idx];
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			for (int i = 0; i < Vin.size(); ++ i) latestArrivalTime[i] = Vin[i];
			qvalue()[0] = 0;
		}
		else
		{
			//
			if (superstep() > queryContainer[2])
			{
				vote_to_halt();
				return;
			}
			
			vector<int> changed;
			for (int i = 0; i < messages.size(); ++ i)
			{
				std::map<int,int>::iterator it;
				it = mIn.lower_bound(messages[i][0]);
				int idx = it->second;
				if (latestArrivalTime[idx] < messages[i][1])
				{
					latestArrivalTime[idx] = messages[i][1];
					changed.push_back(idx);
					if (Vin[idx] - latestArrivalTime[idx] < qvalue()[0]) qvalue()[0] = Vin[idx] - latestArrivalTime[idx]; //store the fastest
				} 
			}
			std::sort(changed.begin(), changed.end());
			std::vector<int>::iterator it;
			it = std::unique(changed.begin(), changed.end());
			changed.resize(std::distance(changed.begin(),it));
			if (changed.size() > 0)
			{
				int currMax = latestArrivalTime[changed[0]];
				for (int i = 1; i < changed.size(); ++ i)
				{
					currMax = max(currMax, latestArrivalTime[changed[i]]);
					latestArrivalTime[changed[i]] = currMax;
				}
				std::map<int,int>::iterator itOut = mOut.lower_bound(Vin[changed[0]]);
				if (itOut != mOut.end())
				{
					int start = itOut->second;
					int shouldSend = latestArrivalTime[changed[0]];
					int pt = 0;
					vector<int> send(2);
					for (int i = start; i < Vout.size() && Vout[i] < t2; ++ i)
					{
						while(pt < changed.size() && Vin[changed[pt]] <= Vout[i]) 
						{
							shouldSend = max(shouldSend, latestArrivalTime[changed[pt]]);
							pt++;
						}
						if (shouldSend > lastOut[i])
						{
							lastOut[i] = shouldSend;
							send[1] = shouldSend;
							for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
							{
								send[0] = Vout[i] + VoutNeighbors[i][j].w;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == KHOP_SHORTEST) //k-hop application
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		
		if (superstep() == 1)
		{
			std::map<int,int>::iterator it,it1,it2;
			it1 = mOut.lower_bound(t1);
			it2 = mOut.upper_bound(t2);
			int idx;
			vector<int> send(2);
			for (it = it1; it != mOut.end() && it != it2; ++ it)
			{
				idx = it->second;
				for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
				{
					send[0] = Vout[idx] + VoutNeighbors[idx][j].w;
					send[1] = VoutNeighbors[idx][j].w;
					send_message(VoutNeighbors[idx][j].v, send);
				}
			}
			for (int i = 0; i < Vin.size(); ++ i) latestArrivalTime[i] = 0;
			qvalue()[0] = 0;
		}
		else
		{
			//
			if (superstep() > queryContainer[2])
			{
				vote_to_halt();
				return;
			}
				
			vector<int> changed;
			for (int i = 0; i < messages.size(); ++ i)
			{
				std::map<int,int>::iterator it;
				it = mIn.lower_bound(messages[i][0]);
				int idx = it->second;
				if (messages[i][1] < latestArrivalTime[idx])
				{
					latestArrivalTime[idx] = messages[i][1];
					changed.push_back(idx); 
					if (latestArrivalTime[idx] < qvalue()[0]) qvalue()[0] = latestArrivalTime[idx]; //store the shortest
				} 
			}
			std::sort(changed.begin(), changed.end());
			std::vector<int>::iterator it;
			it = std::unique(changed.begin(), changed.end());
			changed.resize(std::distance(changed.begin(),it));
			if (changed.size() > 0)
			{
				int currMin = latestArrivalTime[changed[0]];
				for (int i = 1; i < changed.size(); ++ i)
				{
					currMin = min(currMin, latestArrivalTime[changed[i]]);
					latestArrivalTime[changed[i]] = currMin;
				}
				std::map<int,int>::iterator itOut = mOut.lower_bound(Vin[changed[0]]);
				if (itOut != mOut.end())
				{
					int start = itOut->second;
					int shouldSend = latestArrivalTime[changed[0]];
					int pt = 0;
					vector<int> send(2);
					for (int i = start; i < Vout.size() && Vout[i] < t2; ++ i)
					{
						while(pt < changed.size() && Vin[changed[pt]] <= Vout[i]) 
						{
							shouldSend = min(shouldSend, latestArrivalTime[changed[pt]]);
							pt++;
						}
						if (shouldSend < lastOut[i])
						{
							lastOut[i] = shouldSend;
							for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
							{
								send[0] = Vout[i] + VoutNeighbors[i][j].w;
								send[1] = shouldSend + VoutNeighbors[i][j].w;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == INTERSECT)
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		if (superstep() == 1)
		{
			if (id == queryContainer[1]) //u
			{
				std::map<int,int>::iterator it,it1,it2;
				it1 = mOut.lower_bound(t1);
				it2 = mOut.upper_bound(t2);
				int idx;
				vector<int> send(1);
				for (it = it1; it != mOut.end() && it != it2; ++ it)
				{
					idx = it->second;
					for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
					{
						send[0] = id;
						send_message(VoutNeighbors[idx][j].v, send);
					}
				}
			}
			else if (id == queryContainer[2]) //v
			{
				std::map<int,int>::iterator it,it1,it2;
				it1 = mIn.lower_bound(t1);
				it2 = mIn.upper_bound(t2);
				int idx;
				vector<int> send(1);
				for (it = it1; it != mIn.end() && it != it2; ++ it)
				{
					idx = it->second;
					for (int j = 0; j < VinNeighbors[idx].size(); ++ j)
					{
						send[0] = id;
						send_message(VinNeighbors[idx][j].v, send);
					}
				}
			}
		}
		else
		{
			bool getU = 0;
			bool getV = 0;
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] == queryContainer[1]) getU = 1;
				if (messages[i][0] == queryContainer[2]) getV = 1;
			}
			if (getU == 1 && getV == 1)
			{
				qvalue()[0] = 1; //to represent intersect
				//cout << id << endl;
			}
		}
		vote_to_halt();
	}
	else if (queryContainer[0] == MIDDLE)
	{
		int t1 = 0; int t2 = inf;
		if (queryContainer.size() == 5) {t1 = queryContainer[3]; t2 = queryContainer[4];}
		if (superstep() == 1)
		{
			if (id == queryContainer[1]) //u
			{
				std::map<int,int>::iterator it,it1,it2;
				it1 = mOut.lower_bound(t1);
				it2 = mOut.upper_bound(t2);
				int idx;
				vector<int> send(2);
				for (it = it1; it != mOut.end() && it != it2; ++ it)
				{
					idx = it->second;
					for (int j = 0; j < VoutNeighbors[idx].size(); ++ j)
					{
						send[0] = id;
						send[1] = Vout[idx]+VoutNeighbors[idx][j].w;
						send_message(VoutNeighbors[idx][j].v, send);
					}
				}
			}
			else if (id == queryContainer[2]) //v
			{
				std::map<int,int>::iterator it,it1,it2;
				it1 = mIn.lower_bound(t1);
				it2 = mIn.upper_bound(t2);
				int idx;
				vector<int> send(2);
				for (it = it1; it != mIn.end() && it != it2; ++ it)
				{
					idx = it->second;
					for (int j = 0; j < VinNeighbors[idx].size(); ++ j)
					{
						send[0] = id;
						send[1] = Vin[idx]-VinNeighbors[idx][j].w;
						send_message(VinNeighbors[idx][j].v, send);
					}
				}
			}
		}
		else
		{
			int getU = inf;
			int getV = -1;
			for (int i = 0; i < messages.size(); ++ i)
			{
				if (messages[i][0] == queryContainer[1] && messages[i][1] < getU) getU = messages[i][1];
				if (messages[i][0] == queryContainer[2] && messages[i][1] > getV) getV = messages[i][1];
			}
			if (getU <= getV)
			{
				qvalue()[0] = 1; //to represent intersect
				//cout << id << endl;
			}
		}
		vote_to_halt();
	}
	
	else if (queryContainer[0] == TEST)
	{
		if (superstep() == 1)
		{
			
			/*
			cout << "id: " << id << endl;
			for (int i = 0; i < Vin.size(); ++ i)
			{
				cout << Vin[i] << ":\n";
				cout << "topo: " << topologicalLevelIn[i] << endl;
				for (int j = 0; j < LinIn[i].size(); ++ j) cout << "(" << LinIn[i][j].first << ", " << LinIn[i][j].second << ") ";
				cout << endl;
				for (int j = 0; j < LoutIn[i].size(); ++ j) cout << "(" << LoutIn[i][j].first << ", " << LoutIn[i][j].second << ") ";
				cout << endl;
			}
			for (int i = 0; i < Vout.size(); ++ i)
			{
				cout << Vout[i] << ":\n";
				cout << "topo: " << topologicalLevelOut[i] << endl;
				for (int j = 0; j < LinOut[i].size(); ++ j) cout << "(" << LinOut[i][j].first << ", " << LinOut[i][j].second << ") ";
				cout << endl;
				for (int j = 0; j < LoutOut[i].size(); ++ j) cout << "(" << LoutOut[i][j].first << ", " << LoutOut[i][j].second << ") ";
				cout << endl;
			}
			*/
			/*
			for (int i = 0; i < Vin.size(); ++ i)
			{
				cout << id << " " << Vin[i] << " " << topologicalLevelIn[i]  << endl;
			}
			for (int i = 0; i < Vout.size(); ++ i)
			{
				cout << id << " " << Vout[i] << " " << topologicalLevelOut[i]  << endl;
			}
			*/
			/*
			countOut = 0;
			for (int i = 0; i < Vin.size(); ++ i) countOut += (LinIn[i].size() + LoutIn[i].size());
			for (int i = 0; i < Vout.size(); ++ i) countOut += (LinOut.size() + LoutOut[i].size());
			*/
		}
		vote_to_halt();
	}
}

class temporalAggregator : public Aggregator<temporalVertex, vector<int>, int> {
public:
	vector<int> collect;
	int aggValue;
	int k; //query specific
	
	virtual void init(){}
	virtual void init(int val_k){
		collect.resize(0);
		k = val_k+1;
	}
	
	virtual void stepPartial(temporalVertex* v){
		int& tmp = *((int*)(v->get_agg()));
		if (v->qvalue()[0] <= tmp)
		collect.push_back(v->qvalue()[0]);
	}
	virtual vector<int>* finishPartial(){
		std::sort(collect.begin(), collect.end());
		if (collect.size() > k) collect.resize(k);
		return &collect;
	}
	virtual void stepFinal(vector<int>* p){
		for (int i = 0; i < p->size(); i ++){
			collect.push_back( (*p)[i] );
		}
	}
	virtual int* finishFinal(){
		sort(collect.begin(), collect.end());
		/*
		cout << "*" << endl;
		for (int i = 0; i < collect.size(); ++ i)
		{
			cout << collect[i] << " ";
		}
		cout << endl;
		*/
		if (k <= collect.size()) aggValue = collect[k-1];
		else aggValue = inf;
		return &aggValue;
	}
};

//define worker class
class temporalWorker : public WorkerOL<temporalVertex, temporalAggregator>{
public:
	char buf[50];
	
	temporalWorker():WorkerOL<temporalVertex, temporalAggregator>(true){}
	
	virtual temporalVertex* toVertex(char* line)
	{
		temporalVertex* v = new temporalVertex;
		istringstream ssin(line);
		int from, num_of_neighbors, to, num_of_t, w, t;
        ssin >> from >> num_of_neighbors;
        v->id = from;
        v->nqvalue().neighbors.resize(num_of_neighbors);
        v->nqvalue().tw.resize(num_of_neighbors);
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
        	ssin >> to >> num_of_t >> w;
        	v->nqvalue().neighbors[i] = to;
        	for (int j = 0; j < num_of_t; ++ j)
        	{
        		ssin >> t;
        		v->nqvalue().tw[i].push_back(make_pair(t,w));
        	}
        }
        return v;
	}
	//Step 6.2: UDF: query string -> query (src_id)
    virtual vector<int> toQuery(char* line)
    {
        //query type is vector<int> 
        vector<int> ret;
        istringstream ssin(line);
        int tmp;
        while(ssin >> tmp) ret.push_back(tmp);
        
        return ret;
    }

    //Step 6.3: UDF: vertex init
    virtual void init(VertexContainer& vertex_vec)    
    {
	    //int src = get_query().v1;
        //vector <int> tt = get_query();
        int src;
        int pos = get_vpos(src);
        if (pos != -1)
            activate(pos);
    }
    virtual void task_init()
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
	virtual void dump(temporalVertex* vertex, BufferedWriter& writer)
	{
		TaskT& task=*(TaskT*)query_entry();
		if (task.query[0] == EARLIEST_QUERY || task.query[0] == LATEST_QUERY ||task.query[0] == FASTEST_QUERY || task.query[0] == SHORTEST_QUERY
		|| task.query[0] == TOPKNEIGHBORS_EARLIEST || task.query[0] == TOPKNEIGHBORS_FASTEST 
		|| task.query[0] == TOPKNEIGHBORS_SHORTEST || task.query[0] == TOPKNEIGHBORS_LATEST
		|| task.query[0] == KHOP_EARLIEST || task.query[0] == KHOP_FASTEST 
		|| task.query[0] == KHOP_SHORTEST || task.query[0] == KHOP_LATEST)
		{
			//if (vertex->id == task.query[1]) cout << vertex->id << " " << 0 << endl;
			//else cout << vertex->id << " " << vertex->qvalue()[0] << endl;
			if (vertex->id == task.query[1]) sprintf(buf, "%d %d\n", vertex->id, 0);
			else sprintf(buf, "%d %d\n", vertex->id, vertex->qvalue()[0]);
			writer.write(buf);
		}
		
		if (task.query[0] == MIDDLE || task.query[0] == INTERSECT)
		{
			if (vertex->qvalue()[0] == 1)
			{
				sprintf(buf, "%d\n", vertex->id);
				writer.write(buf);
			}
		}
		//neighbors
		if (task.query[0] == OUT_NEIGHBORS_QUERY)
		{
			int t1 = 0; int t2 = inf;
			if (task.query.size() == 4) {t1 = task.query[2]; t2 = task.query[3];}
			std::map<int,int>::iterator it,it1,it2;
			it1 = vertex->mOut.lower_bound(t1);
			it2 = vertex->mOut.upper_bound(t2);
			int idx;
			for (it = it1; it!=vertex->mOut.end() && it!=it2; ++it)
			{
				idx = it->second;
				for (int j = 0; j < vertex->VoutNeighbors[idx].size(); ++ j)
				{
					//cout << "(" << vertex->VoutNeighbors[idx][j].v << " " << vertex->Vout[idx] << " " << vertex->VoutNeighbors[idx][j].w << ")"<< endl;
					sprintf(buf, "%d %d %d\n", vertex->VoutNeighbors[idx][j].v, vertex->Vout[idx], vertex->VoutNeighbors[idx][j].w);
					writer.write(buf);
				}
			}
		}
		if (task.query[0] == IN_NEIGHBORS_QUERY)
		{
			int t1 = 0; int t2 = inf;
			if (task.query.size() == 4) {t1 = task.query[2]; t2 = task.query[3];}
			std::map<int,int>::iterator it,it1,it2;
			it1 = vertex->mIn.lower_bound(t1);
			it2 = vertex->mIn.upper_bound(t2);
			int idx;
			for (it = it1; it!=vertex->mIn.end() && it!=it2; ++it)
			{
				idx = it->second;
				for (int j = 0; j < vertex->VinNeighbors[idx].size(); ++ j)
				{
					//cout << "(" << vertex->VinNeighbors[idx][j].v << " " << vertex->Vin[idx] << " " << vertex->VinNeighbors[idx][j].w << ")"<< endl;
					sprintf(buf, "%d %d %d\n", vertex->VinNeighbors[idx][j].v, vertex->Vin[idx], vertex->VinNeighbors[idx][j].w);
					writer.write(buf);
				}
			}
		}
		//reachability
		if (task.query[0] == REACHABILITY_QUERY_TOPCHAIN || task.query[0] == REACHABILITY_QUERY)
		{
			if (vertex->qvalue()[0] < 0) 
			{
				sprintf(buf, "1\n");
				writer.write(buf);
			}
		}
		if (task.query[0] ==  EARLIEST_QUERY_TOPCHAIN)
		{
			if (vertex->qvalue()[0] < 0) 
			{
				sprintf(buf, "%d\n", -vertex->qvalue()[0]);
				writer.write(buf);
			}
		}
		
		if (task.query[0] == TEST)
		{
			int count1 = 0;
			for (int i = 0; i < vertex->Vin.size(); ++ i) count1 += (vertex->LinIn[i].size() + vertex->LoutIn[i].size());
			for (int i = 0; i < vertex->Vout.size(); ++ i) count1 += (vertex->LinOut[i].size() + vertex->LoutOut[i].size());
			sprintf(buf, "%d\n", count1);
			writer.write(buf);
			/*
			for (int i = 0; i < vertex->Vin.size(); ++ i)
			{	
				sprintf(buf, "%d %d %d\n", vertex->id, vertex->Vin[i], vertex->topologicalLevelIn[i]);
				writer.write(buf);
			}
			for (int i = 0; i < vertex->Vout.size(); ++ i)
			{	
				sprintf(buf, "%d %d %d\n", vertex->id, vertex->Vout[i], vertex->topologicalLevelOut[i]);
				writer.write(buf);
			}
			*/
		}
	}
	
	virtual void global_compute()
	{
		call_operator("1 0");
		call_operator("1 2");
	}
};

class pathCombiner : public Combiner<vector<int> > {
public:
    virtual void combine(vector<int>& old, const vector<int>& new_msg)
    {
        if (old[0] > new_msg[0]) old = new_msg;
    }
};
int main(int argc, char* argv[])
{
	in_path = argv[1];
	
	WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    temporalWorker worker;
    pathCombiner combiner;
    worker.setCombiner(&combiner);
    
    worker.run(param);
    return 0;
}
