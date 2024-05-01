#include<bits/stdc++.h>
using namespace std;
using lli=long long int;
map<int,int>mp;
int n,k;
bool check(int mid)
{
    lli balls_requ=1LL*mid*k;
    lli balls_gain=0;
    for(auto v:mp)
    {
        balls_gain+=min(v.second,mid);
    }
    if(balls_gain>=balls_requ)
    return 1;
    return 0;
}
void solve(){
    cin>>n>>k;
    mp.clear();
    for(int i=0;i<n;i++){
        int temp;
        cin>>temp;
        mp[temp]++;
    }
    lli low=1;
    lli high=n;
    lli ans=0;
    while(low<=high){
        lli mid=(high+low)/2;
        if(check(mid)){
           ans=mid;
           low=mid+1;
            
        }
        else{
            high=mid-1;
        }
    }
    cout<<ans<<endl;
    
}
int main(){
    ios_base::sync_with_stdio(0);
    cin.tie(0);
    cout.tie(0);
    int t;
    cin>>t;
    while(t--){
        solve();
    }
    return 0;
}