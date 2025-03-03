// $ go run client/main.go  コマンドでアクションを起こす

package main

import (
	"context"
	"fmt"
	"grpc-lesson/pb"
	"io"
	"log"
	"os"
	"time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func main() {
	//SSL通信
	certFile := "/Users/tanakahiroshi/Library/Application Support/mkcert/rootCA.pem"
	creds, err := credentials.NewClientTLSFromFile(certFile, "")  //credentialを取得

	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(creds))  //grpc.WithInsecure()を使用すると暗号化通信なし
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewFileServiceClient(conn)
	// callListFiles(client)
	callDownload(client)
	// CallUpload(client)
	// CallUploadAndNotifyProgress(client)
}

// Unary RPC
func callListFiles(client pb.FileServiceClient) {
	md := metadata.New(map[string]string{"authorization": "Bearer test-token"})  // 認証時
	ctx := metadata.NewOutgoingContext(context.Background(), md)  // 認証時
	res, err := client.ListFiles(ctx, &pb.ListFilesRequest{})  // 認証時
	// res, err := client.ListFiles(context.Background(), &pb.ListFilesRequest{})
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(res.GetFileNames())  //ファイル名一覧
}

// Server Streaming RPC
func callDownload(client pb.FileServiceClient) {
	//deadline
	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	req := &pb.DownloadRequest{FileName: "name.txt"}
	stream, err := client.Download(ctx, req)  //ダウンロードメソッドをコールする
	if err != nil {
		log.Fatalln(err)
	}

	for {
		res, err := stream.Recv()  //サーバーがレスポンスを返すたびにその内容をresで受け取ることができる
		if err == io.EOF {
			break
		}
		if err != nil {
			resErr, ok := status.FromError(err)  //エラーがgrpcのエラーの場合は戻り値のokがtrueになる
			if ok {
				if resErr.Code() == codes.NotFound {
					log.Fatalf("Error Code: %v, Error Message: %v", resErr.Code(), resErr.Message())
				} else if resErr.Code() == codes.DeadlineExceeded {
					log.Fatalln("deadline exceeded")
				} else {
					log.Fatalln("unknown grpc error")
				}
			} else {
				log.Fatalln(err)
			}
		}

		log.Printf("Response from Download(bytes): %v", res.GetData())
		log.Printf("Response from Download(string): %v", string(res.GetData()))
	}
}

// Client Streaming RPC
func CallUpload(client pb.FileServiceClient) {
	filename := "sports.txt"
	path := "/Users/tanakahiroshi/Desktop/Programming/GO/grpc/workspace/grpc-lesson/storage/" + filename

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	stream, err := client.Upload(context.Background())  //streamを取得
	if err != nil {
		log.Fatalln(err)
	}

	buf := make([]byte, 5)
	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}

		req := &pb.UploadRequest{Data: buf[:n]}
		sendErr := stream.Send(req)
		if sendErr != nil {
			log.Fatalln(sendErr)
		}

		time.Sleep(1 * time.Second)
	}

	res, err := stream.CloseAndRecv()  //リクエストの終了をサーバーに通知し、レスポンスを受け取ることができる
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("received data size: %v", res.GetSize())
}

// Bidirectional Streaming RPC
func CallUploadAndNotifyProgress(client pb.FileServiceClient) {
	filename := "sports.txt"
	path := "/Users/tanakahiroshi/Desktop/Programming/GO/grpc/workspace/grpc-lesson/storage/" + filename

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	stream, err := client.UploadAndNotifyProgress(context.Background())  //ストリームを取得
	if err != nil {
		log.Fatalln(err)
	}

	// request
	buf := make([]byte, 5 )
	go func ()  {
		for {
			n, err := file.Read(buf)  //ファイルの内容をバッファーに格納
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}

			req := &pb.UploadAndNotifyProgressRequest{Data: buf[:n]}
			sendErr := stream.Send(req)
			if sendErr != nil {
				log.Fatalln(sendErr)
			}
			time.Sleep(1 * time.Second)
		}

		err := stream.CloseSend()
		if err != nil {
			log.Fatalln(err)
		}
	} ()

	// response
	ch := make(chan struct{})
	go func () {
		for {
			res, err := stream.Recv()  //サーバーからのレスポンスを受け取る
			if err == io.EOF {  //エンドオブファイルが到達したら
				break
			}
			if err != nil {
				log.Fatalln(err)
			}

			log.Printf("received message: %v", res.GetMsg())
		}
		close(ch)
	} ()
	<-ch
}