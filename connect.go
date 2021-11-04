package main

import (
	"fmt"
	"os"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/client"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"

	"google.golang.org/grpc"
)

var stargateClient *client.StargateClient

func main() {

	grpcEndpoint := "localhost:8090"
	authEndpoint := "localhost:8081"

	conn, err := grpc.Dial(grpcEndpoint, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithPerRPCCredentials(
			auth.NewTableBasedTokenProviderUnsafe(
				fmt.Sprintf("http://%s/v1/auth", authEndpoint), "cassandra", "cassandra",
			),
		),
	)

	// grpcEndpoint := "a2b4465c-e7a4-4cb7-a4a4-c829f0ef10d6-us-west1.apps.astra.datastax.com:443"

	// config := &tls.Config{
	// 	InsecureSkipVerify: false,
	// }

	// conn, err := grpc.Dial(grpcEndpoint, grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithBlock(),
	// 	grpc.WithPerRPCCredentials(
	// 		auth.NewStaticTokenProvider("AstraCS:uuwizlOZhGxrUxaOqHPLAGCK:b4296e99a9f801d78043272b0efd79dca115b1fd95765780df36ed3ada87ff9b"),
	// 	),
	// )

	stargateClient, err = client.NewStargateClientWithConn(conn)

	if err != nil {
		fmt.Printf("error creating client %v", err)
		os.Exit(1)
	}
	// if err != nil {
	// 	fmt.Printf("error dialing connection %v", err)
	// 	os.Exit(1)
	// }
	// defer conn.Close()
	fmt.Printf("made client\n")

	// Create a new keyspace
	createKeyspaceStatement := &pb.Query{
		Cql: "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
	}

	_, err = stargateClient.ExecuteQuery(createKeyspaceStatement)
	// if err != nil {
	// 	return err
	// }
	fmt.Printf("made keyspace\n")

	// Create a new table
	createTableQuery := &pb.Query{
		Cql: "CREATE TABLE IF NOT EXISTS test.users (firstname text PRIMARY KEY, lastname text);",
	}

	_, err = stargateClient.ExecuteQuery(createTableQuery)
	// if err != nil {
	// 	return err
	// }
	fmt.Printf("made table \n")

	batch := &pb.Batch{
		Type: pb.Batch_LOGGED,
		Queries: []*pb.BatchQuery{
			{
				Cql: "INSERT INTO test.users (firstname, lastname) VALUES ('Lorina', 'Poland');",
			},
			{
				Cql: "INSERT INTO test.users (firstname, lastname) VALUES ('Ronnie', 'Miller');",
			},
		},
	}

	_, err = stargateClient.ExecuteBatch(batch)

	selectQuery := &pb.Query{
		Cql: "SELECT firstname, lastname FROM test.users;",
	}

	response, err := stargateClient.ExecuteQuery(selectQuery)
	if err != nil {
		fmt.Printf("error executing query %v", err)
		os.Exit(1)
	}

	result := response.GetResultSet()

	// NOT WORKING
	// var i, j int
	// for i = 0; i < 2; i++ {
	// 	for j = 1; j < 2; j++ {
	// 		firstname, err := client.ToString(result.Rows[i].Values[i])
	// 		if err != nil {
	// 			fmt.Printf("error getting firstname %v", err)
	// 			os.Exit(1)
	// 		}
	// 		lastname, err := client.ToString(result.Rows[i].Values[j])
	// 		if err != nil {
	// 			fmt.Printf("error getting lastname %v", err)
	// 			os.Exit(1)
	// 		}
	// 		fmt.Printf("%v %v \n", firstname, lastname)
	// 	}
	// }

	firstname, err := client.ToString(result.Rows[0].Values[0])
	if err != nil {
		fmt.Printf("error getting firstname %v", err)
		os.Exit(1)
	}

	lastname, err := client.ToString(result.Rows[0].Values[1])
	if err != nil {
		fmt.Printf("error getting lastname %v", err)
		os.Exit(1)
	}

	fmt.Printf("%v %v \n", firstname, lastname)

	firstname2, err := client.ToString(result.Rows[1].Values[0])
	if err != nil {
		fmt.Printf("error getting firstname %v \n", err)
		os.Exit(1)
	}

	lastname2, err := client.ToString(result.Rows[1].Values[1])
	if err != nil {
		fmt.Printf("error getting lastname %v \n", err)
		os.Exit(1)
	}

	fmt.Printf("%v %v \n", firstname2, lastname2)
}
