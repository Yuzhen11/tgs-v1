#ifndef VERTEXOL_H
#define VERTEXOL_H

#include "utils/global.h" //for msg_buf when sending msgs
#include "global_ol.h"//for query_info during compute()
#include <vector>
#include "basic/Vertex.h"//for DefaultHash
#include "smpair.h"
#include "MessageBufferOL.h"
using namespace std;

//1. QValue + NQValue
//2. QValue is a hashmap<qid, (vertex_state, msg_buf, is_active)>
//3. getPair() caches vertex[qid] to avoid repeated hashtable lookup, and auto-generate init entry

template<class KeyT, class QValueT, class NQValueT, class MessageT,
		class QueryT, class HashT = DefaultHash<KeyT> >
class VertexOL {
	//QValueT: value part that changes with different queries
	//NQValueT: fixed part, like adj-list(s)
	public:

		typedef KeyT KeyType;
		typedef QValueT QValueType;
		typedef NQValueT NQValueType;
		typedef MessageT MessageType;
		typedef QueryT QueryType;
		typedef HashT HashType;
		typedef vector<MessageType> MessageContainer;
		typedef typename MessageContainer::iterator MessageIter;
		typedef VertexOL<KeyT, QValueT, NQValueT, MessageT, QueryT, HashT> VertexT;
		typedef MessageBufferOL<VertexT> MessageBufT;

		typedef SMPair<QValueT, MessageT> SMPairT;
		//typedef hash_map<int, SMPairT*> SMPMap;
		typedef std::map<int, SMPairT*> SMPMap;
		typedef typename SMPMap::iterator SMPMapIter;

		typedef Task<VertexT> TaskT;
		typedef hash_map<int, TaskT> QMap;

		KeyT id;
		NQValueT _value;
		SMPMap map; //map[qid] = smpair for current query

		//========================================
		//UDF
		virtual QValueT init_value(QueryT& query) = 0;
		virtual void compute(MessageContainer& messages) = 0;//system-wise, call vertex_compute() below
		
		
		//
		//
	    virtual void preCompute(MessageContainer& messages, int phaseNum) = 0;
	    
		//========================================
		static void forceTerminate()
		{
			TaskT& task=*(TaskT*)query_entry();
			task.forceTerminate();
		}
		static void canVisit()
		{
			TaskT& task=*(TaskT*)query_entry();
			task.canVisit();
			task.visit = 1;
		}

		//========================================
		//1. caching to avoid looking up "map" many times for the same query
		//2. do init_value() for the first access to a vertex's smpair (must be active)
		//BEGIN
		int cached_qid;
		SMPairT* cached_pair;
		//------
		SMPairT* getPair()
		{
			int qid=query_id();
			if(cached_qid!=qid)
			{
				cached_qid=qid;
				SMPMapIter it=map.find(qid);
				if(it==map.end())
				{
					TaskT& task=*(TaskT*)query_entry();
					QValueT init_val=init_value(task.query);
					SMPairT* pair=new SMPairT(init_val, true);
					cached_pair=map[qid]=pair;
				}
				else
				{
					cached_pair=it->second;
				}
			}
			return cached_pair;
		}
		//END

		VertexOL()
		{
			cached_qid=-1;
		}

		~VertexOL()
		{
			for(SMPMapIter it=map.begin(); it!=map.end(); it++)
			{
				delete it->second;
			}
		}

		//========================================

		friend ibinstream& operator<<(ibinstream& m, const VertexT& v)
		{
			m << v.id;
			m << v._value;
			return m;
		}
		friend obinstream& operator>>(obinstream& m, VertexT& v)
		{
			m >> v.id;
			m >> v._value;
			return m;
		}

		//========================================

		static QueryT* get_query()//called in compute()
		{
			TaskT& task=*(TaskT*)query_entry();
			return &task.query;
		}

		inline NQValueT& nqvalue()
		{
			return _value;
		}
		inline const NQValueT& nqvalue() const
		{
			return _value;
		}

		inline QValueT& qvalue()
		{
			return getPair()->value();
		}
		inline const QValueT& qvalue() const
		{
			return getPair()->value();
		}

		inline MessageContainer& mbuf()
		{
			return getPair()->mbuf();
		}

		inline const MessageContainer& mbuf() const
		{
			return getPair()->mbuf();
		}

		void vertex_compute()
		{
			MessageContainer& inbuf = mbuf();
			compute(inbuf);
			inbuf.clear();//empty the in-buf
			
		}
		
		void vertex_pre_compute(int phaseNum)
		{
			MessageContainer& inbuf = mbuf();
			preCompute(inbuf, phaseNum);
			inbuf.clear();//empty the in-buf
			
		}
		

		//========================================

		inline bool operator<(const VertexT& rhs) const
		{
			return id < rhs.id;
		}
		inline bool operator==(const VertexT& rhs) const
		{
			return id == rhs.id;
		}
		inline bool operator!=(const VertexT& rhs) const
		{
			return id != rhs.id;
		}

		//========================================
		//WorkerOL (1)calls vertex_compute(); (2)checks is_active() to decide whether to add to scheduler

		inline bool is_active()
		{
			return getPair()->active;
		}

		inline void activate()
		{
			getPair()->active = true;
		}

		inline void vote_to_halt()
		{
			getPair()->active = false;
		}

		//========================================

		static int superstep()
		{
			TaskT& task=*(TaskT*)query_entry();
			return task.superstep;
		}
		static int restart()
		{
			TaskT& task=*(TaskT*)query_entry();
			return task.restart;
		}
		static int getrsrc()
		{
			TaskT& task=*(TaskT*)query_entry();
			return task.rsrc;
		}

		static void* get_agg()
		{
			TaskT& task=*(TaskT*)query_entry();
			return task.agg;
		}

		void send_message(const KeyT& id, const MessageT& msg)
		{
			((MessageBufT*)get_message_buffer())->add_message(id, msg);
		}

		//========================================

		void free()//must have the query_entry before calling
		{
			SMPMapIter it=map.find(query_id());
			delete it->second;
			map.erase(it);
		}
};

#endif
