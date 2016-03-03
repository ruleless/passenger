class BaseServerGroup {
public:
	typedef boost::container::small_vector<4, ServerPtr> SmallServerList;

	mutex;
	unsigned int number;
	string serversHash;

	SmallServerList servers;
	SmallServerList balancingList;
	unsigned int nextServer;
	bool allHealthy;
};
