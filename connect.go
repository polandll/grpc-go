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

	// Stargate OSS configuration for locally hosted docker image
	stargateUri := "localhost:8090"
	authEndpoint := "localhost:8081"
	username := "cassandra"
	password := "cassandra"

	// Astra DB configuration
	// astraUri := "$ASTRA_CLUSTER_ID-$ASTRA_REGION.apps.astra-dev.datastax.com:443"
	// bearer_token := "AstraCS:xxxxx"

	// Create connection with authentication
	// For Stargate  OSS:
	conn, err := grpc.Dial(stargateUri, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithPerRPCCredentials(
			auth.NewTableBasedTokenProviderUnsafe(
				fmt.Sprintf("http://%s/v1/auth", authEndpoint), username, password,
			),
		),
	)

	// For Astra DB:
	// config := &tls.Config{
	// 	InsecureSkipVerify: false,
	// }

	// conn, err := grpc.Dial(astraUri, grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithBlock(),
	// 	grpc.WithPerRPCCredentials(
	// 		// auth.NewStaticTokenProvider("AstraCS:uuwizlOZhGxrUxaOqHPLAGCK:b4296e99a9f801d78043272b0efd79dca115b1fd95765780df36ed3ada87ff9b"),
	// 		auth.NewStaticTokenProvider(bearer_token),
	// 	),
	// )

	// For Stargate OSS and Astra DB: Create the gRPC client
	stargateClient, err = client.NewStargateClientWithConn(conn)

	if err != nil {
		fmt.Printf("error creating client %v", err)
		os.Exit(1)
	}

	fmt.Printf("made client\n")

	// For Stargate OSS: Create a new keyspace
	// For Astra DB: delete these statements and create a keyspace in the Astra DB dashboard
	createKeyspaceStatement := &pb.Query{
		Cql: "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
	}

	_, err = stargateClient.ExecuteQuery(createKeyspaceStatement)
	if err != nil {
		fmt.Printf("error creating keyspace %v", err)
		return
	}

	fmt.Printf("made keyspace\n")

	// For Stargate OSS and Astra DB: Create a new table
	createTableQuery := &pb.Query{
		Cql: "CREATE TABLE IF NOT EXISTS test.users (firstname text PRIMARY KEY, lastname text);",
	}

	_, err = stargateClient.ExecuteQuery(createTableQuery)
	if err != nil {
		fmt.Printf("error creating table %v", err)
		return
	}

	fmt.Printf("made table \n")

	// For Stargate OSS and Astra DB: INSERT two rows/records
	//  Two queries will be run in a batch statement
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
	if err != nil {
		fmt.Printf("error creating batch %v", err)
		return
	}

	fmt.Printf("insert data\n")

	// For Stargate OSS and Astra DB: SELECT the data to read from the table
	selectQuery := &pb.Query{
		Cql: "SELECT firstname, lastname FROM test.users;",
	}

	response, err := stargateClient.ExecuteQuery(selectQuery)
	if err != nil {
		fmt.Printf("error executing query %v", err)
		return
	}

	fmt.Printf("select executed\n")

	// Get the results from the execute query statement
	result := response.GetResultSet()

	// This for loop gets 2 results
	var i, j int
	for i = 0; i < 2; i++ {
		valueToPrint := ""
		for j = 0; j < 2; j++ {
			value, err := client.ToString(result.Rows[i].Values[j])
			if err != nil {
				fmt.Printf("error getting value %v", err)
				os.Exit(1)
			}
			valueToPrint += " "
			valueToPrint += value
		}
		fmt.Printf("%v \n", valueToPrint)
	}
	fmt.Printf("everything worked!")
}
