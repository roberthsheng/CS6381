# Testing Results

Across all the configurations we tested, we had:

3 Discovery Services
3 Brokers
1 Load Balancer
1 Zookeeper instance

The 3 discovery services, 3 brokers, and 1 load balancer were all deployed on different VMs. 

We varied the number of susbcribers and publishers in our testing:

2 subscribers, 2 publishers
5 subscribers, 5 publishers
10 subscribers, 10 publishers.

We got the following results:

2 pub, 2 sub: 5.61 milliseconds
5 pub, 5 sub: 6.33 milliseconds
10 pub, 10 sub: 7.84 milliseconds. 

