#include "tools/msgtool.h"
#include "ydhdfs.h"
#include <iostream>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
using namespace std;

typedef vector<vector<int> > resultVector;

class GtimerOperator
{
public:
    //GtimerOperator();
    GtimerOperator(string outputFile = "");
    void send_msg(char* msg);
    int getQueryNum();
    void changeToString(char* msg);
    resultVector loadResult(string inDir);
    string queryHandler(int n_args, ...);
    void printResult(const resultVector& res);


    void server_exit();

    string out_nb(int u, int t1, int t2);
    string in_nb(int u, int t1, int t2);
    string out_edge(int u, int t1, int t2);
    string in_edge(int u, int t1, int t2);

    string reachability(int u, int v, int t1, int t2);
    string reachability_topChain(int u, int v, int t1, int t2);
    string earliest_topChain(int u, int v, int t1, int t2);
    
    string earliest(int u, int t1, int t2);
    string fastest(int u, int t1, int t2);
    string shortest(int u, int t1, int t2);
    string latest(int u, int t1, int t2);
    
    string topk_earliest(int u, int k, int t1, int t2);
    string topk_fastest(int u, int k, int t1, int t2);
    string topk_shortest(int u, int k, int t1, int t2);
    string topk_latest(int u, int k, int t1, int t2);
    
    string khop(int u, int k, int t1, int t2);
    /*
    string khop_earliest(int u, int k, int t1, int t2);
    string khop_fastest(int u, int k, int t1, int t2);
    string khop_shortest(int u, int k, int t1, int t2);
    string khop_latest(int u, int k, int t1, int t2);
    */
    
    string intersect(int u, int v, int t1, int t2);
    string middle(int u, int v, int t1, int t2);
    
    void addedge(int u, int v, int t1, int t2);

private:
    msg_queue_client client;
    msg_queue_receiver receiver;
    int queryNum;
    int type;
    vector<int> vmsg;
    string outputFile;
};

GtimerOperator::GtimerOperator(string outputFile):outputFile(outputFile)
{
    queryNum = 0;
    type = 1;
}

void GtimerOperator::send_msg(char* msg)
{
    queryNum ++;
    client.send_msg(type, msg);
    
    //waiting for the reply
    while(receiver.recv_msg(type) == false);
    char* notif = receiver.get_msg();
}
int GtimerOperator::getQueryNum()
{
    return queryNum;
}
void GtimerOperator::changeToString(char* msg)
{
    msg[0] = '\0';
    char buf[15];
    for (auto & i : vmsg)
    {
        sprintf(buf,"%d", i); 
        strcat(msg, strcat(buf, " ") );
    }
}
resultVector GtimerOperator::loadResult(string sinDir)
{
    hdfsFS fs = hdfsConnect("master", 9000);
    
    resultVector ret;
    
    const char* inDir = sinDir.c_str();
    int numFiles;
    hdfsFileInfo* fileinfo = hdfsListDirectory(fs, inDir, &numFiles);
    for (int i = 0; i < numFiles; ++ i)
    {
    	//print file
        //cout << fileinfo[i].mName << endl;
        hdfsFile in = hdfsOpenFile(fs, fileinfo[i].mName, O_RDONLY | O_CREAT, 0, 0, 0);
        LineReader reader(fs, in);
        while(1)
        {
            reader.readLine();
            if (!reader.eof()) 
            {
            	ret.resize(ret.size()+1);
            	//cout << reader.getLine() << endl;
            	int tmp;
            	char* line = reader.getLine();
            	istringstream ssin(line);
            	while(ssin >> tmp)
            	{
            		ret.back().push_back(tmp);
            	}
            	
            }
            else break;
        }
    }
    return ret;
}
void GtimerOperator::printResult(const resultVector& res)
{
	cout << "-------result-------" << endl;
	for (int i = 0; i < res.size(); ++ i)
	{
		for (int j = 0; j < res[i].size(); ++ j)
		{
			cout << res[i][j] << " ";
		}
		cout << endl;
	}
}
void GtimerOperator::server_exit()
{
    client.send_msg(type,"server_exit");
}

string GtimerOperator::queryHandler(int n_args, ...)
{
    //push into vmsg
	vmsg.clear();
	va_list ap;
    va_start(ap, n_args);
    for (int i = 1; i <= n_args; i++)
    {
        int tmp = va_arg(ap, int);
        if (tmp != -1)
            vmsg.push_back(tmp);
    }
    va_end(ap);

    //change vmsg into msg
    char msg[80];
    changeToString(msg);
    cout << msg << endl;
    send_msg(msg);
    return outputFile+"/query"+to_string(queryNum+4);
}

//online query
string GtimerOperator::out_nb(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 1, u, t1, t2);
}
string GtimerOperator::in_nb(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 2, u, t1, t2);
}
string GtimerOperator::out_edge(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 41, u, t1, t2);
}
string GtimerOperator::in_edge(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 42, u, t1, t2);
}

string GtimerOperator::reachability(int u, int v, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 3, u, v, t1, t2);
}
string GtimerOperator::reachability_topChain(int u, int v, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 4, u, v, t1, t2);
}
string GtimerOperator::earliest_topChain(int u, int v, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 5, u, v, t1, t2);
}
//analytic query
string GtimerOperator::earliest(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 7, u, t1, t2);
}
string GtimerOperator::fastest(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 8, u, t1, t2);
}
string GtimerOperator::shortest(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 9, u, t1, t2);
}
string GtimerOperator::latest(int u, int t1 = -1, int t2 = -1)
{
	return queryHandler(4, 10, u, t1, t2);
}
//application
string GtimerOperator::topk_earliest(int u, int k,  int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 22, u, k, t1, t2);
}
string GtimerOperator::topk_fastest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 23, u, k, t1, t2);
}
string GtimerOperator::topk_shortest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 24, u, k, t1, t2);
}
string GtimerOperator::topk_latest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 25, u, k, t1, t2);
}
string GtimerOperator::khop(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 26, u, k, t1, t2);
}
/*
string GtimerOperator::khop_earliest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 26, u, k, t1, t2);
}
string GtimerOperator::khop_fastest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 27, u, k, t1, t2);
}
string GtimerOperator::khop_shortest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 28, u, k, t1, t2);
}
string GtimerOperator::khop_latest(int u, int k, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 29, u, k, t1, t2);
}
*/

string GtimerOperator::intersect(int u, int v, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 31, u, v, t1, t2);
}
string GtimerOperator::middle(int u, int v, int t1 = -1, int t2 = -1)
{
	return queryHandler(5, 32, u, v, t1, t2);
}

void GtimerOperator::addedge(int u, int v, int t1 = -1, int t2 = -1)
{
	queryHandler(5, 11, u, v, t1, t2);
}
