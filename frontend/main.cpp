#include "TemporalOperator.h"
using namespace std;

int main()
{
    TemporalOperator op("/yuzhen/output");
    string s1 = op.out_nb(0);
    cout << s1 << endl;
    resultVector res = op.loadResult(s1);
    printresult(res);
    
    string s2 = op.out_nb(1,1,5);
    cout << s2 << endl;
    resultVector res2 = op.loadResult(s2);
    printresult(res2);
    
    resultVector r;
    string s;
    s = op.reachability(0, 3);
    r = op.loadResult(s);
    printresult(r);

    s = op.reachability_topChain(0, 3);
    r = op.loadResult(s);
    printresult(r);
    
    s = op.earliest_topChain(0, 3);
    r = op.loadResult(s);
    printresult(r);

    s = op.earliest(0);
    r = op.loadResult(s);
    printresult(r);

    s = op.fastest(0);
    r = op.loadResult(s);
    printresult(r);

    s = op.shortest(0);
    r = op.loadResult(s);
    printresult(r);

    s = op.latest(0);
    r = op.loadResult(s);
    printresult(r);


    s = op.topk_earliest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.topk_fastest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.topk_shortest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.topk_latest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.khop_earliest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.khop_fastest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.khop_shortest(0, 1);
    r = op.loadResult(s);
    printresult(r);

    s = op.khop_latest(0, 1);
    r = op.loadResult(s);
    printresult(r);
    
    
    s = op.intersect(0, 3);
    r = op.loadResult(s);
    printresult(r);

    s = op.middle(0, 3);
    r = op.loadResult(s);
    printresult(r);

    //exit
    op.server_exit();

    return 0;
}
