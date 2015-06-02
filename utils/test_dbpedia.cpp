#include "triple_debug.h"
#include "fstream"
using namespace std;

int main()
{
	ifstream fin("/data/rdf/btc2012/dbpedia");
	char line[1000000]={0};
	statement o;
	int c=0;
	while(fin.getline(line, 1000000))
	{
		o.parseQuad(line);
		c++;
		if(stmt_error)
		{
			cout<<endl;
			cout<<"Line "<<c<<endl;
			cout<<line<<endl;
			cout<<endl;
		}
		if(c%1000000 == 0) cout<<c<<" lines processed"<<endl;
	}
	fin.close();
	return 0;
}
