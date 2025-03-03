// $ go run server/main.go  コマンドでサーバーを立ち上げる

package main

import (
	"bytes"
	"context"
	"fmt"
	"grpc-lesson/pb"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	grpc_middlewere "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedFileServiceServer
}

// Unary RPC
func (*server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	fmt.Println("ListFiles was invoked")

	dir := "/Users/tanakahiroshi/Desktop/Programming/GO/grpc/workspace/grpc-lesson/storage"

	paths, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, path := range paths {
		if !path.IsDir() {
			filenames = append(filenames, path.Name())  //スライスにfilenameを追加
		}
	}

	res := &pb.ListFilesResponse{
		FileNames: filenames,
	}
	return res, nil
}

// Server Streaming RPC
func (*server) Download(req *pb.DownloadRequest, stream pb.FileService_DownloadServer) error {
	fmt.Println("Download was invoked")  //デバック用の文字列を出力

	filename := req.GetFileName()  //リクエストされたファイル名を取得
	path := "/Users/tanakahiroshi/Desktop/Programming/GO/grpc/workspace/grpc-lesson/storage/" + filename

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return status.Error(codes.NotFound, "file was not found")  //エラーハンドリング
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 5)  //長さが5のバイトがたスライスを用意
	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		res := &pb.DownloadResponse{Data: buf[:n]}
		sendErr := stream.Send(res)
		if sendErr != nil {
			return sendErr
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

// Client Streaming RPC
func (*server) Upload(stream pb.FileService_UploadServer) error {
	fmt.Println("Upload was invoked")

	var buf bytes.Buffer  //クライアントからアップロードされたデータを格納するためのバッファーを用意
	for {
		req, err := stream.Recv()  //クライアント側からstream経由で複数のリクエストを取得することができる
		if err == io.EOF {  //クライアントからの終了信号が到達した場合
			res := &pb.UploadResponse{Size: int32(buf.Len())}
			return stream.SendAndClose(res)
		}
		if err != nil {
			return err
		}

		data := req.GetData()  //リクエストからのデータを変数に格納し、その内容を出力するように
		log.Printf("received data(bytes) %v", data)
		log.Printf("received data(string) %v", string(data))
		buf.Write(data)
	}
}

// Bidirectional Streaming RPC
func (*server) UploadAndNotifyProgress(stream pb.FileService_UploadAndNotifyProgressServer) error {
	fmt.Println("UploadAndNotifyProgress was invoked")

	size := 0

	for {
		req, err := stream.Recv()  //クライアント側からstream経由で複数のリクエストを取得することができる
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		data := req.GetData()
		log.Printf("received data: %v", data)
		size += len(data)  //クライアントから受信したデータのサイズをサイズ変数に足していく

		res := &pb.UploadAndNotifyProgressResponse{
			Msg: fmt.Sprintf("received %vbytes", size),
		}
		err = stream.Send(res)
		if err != nil {
			return err
		}
	}
}

//Interceptor
func myLogging() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		log.Printf("request data: %+v", req)

		resp, err = handler(ctx, req)
		if err != nil {
			return nil, err
		}
		log.Printf("response data: %+v", resp)

		return resp, nil
	}
}

//認証
func authorize(ctx context.Context) (context.Context, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "Bearer")  //MDはメタデータの略で、クライアントから送られてくるコンテキストのメタデータから第二引数で指定したBearerというキーで認証情報を取り出せる
	if err != nil {
		return nil, err
	}

	if token != "test-token" {
		return nil, status.Error(codes.Unauthenticated, "token is invalid")
	}
	return ctx, nil
}

func main() {
	lis, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	//SSL通信
	creds, err := credentials.NewServerTLSFromFile(
		"ssl/localhost.pem",
		"ssl/localhost-key.pem",
	)
	if err != nil {
		log.Fatalln(err)
	}

	// s := grpc.NewServer()
	// s := grpc.NewServer(grpc.UnaryInterceptor(myLogging()))  //Interceptor
	s := grpc.NewServer(  //Interceptor、認証
		grpc.Creds(creds),
		grpc.UnaryInterceptor(
		grpc_middlewere.ChainUnaryServer(  //可変長のUnaryServerInterceptorを引数として取れる
			myLogging(),
			grpc_auth.UnaryServerInterceptor(authorize),
		),
	))
	pb.RegisterFileServiceServer(s, &server{})

	fmt.Println("server is running...")
	if err := s.Serve(lis); err != nil {  //指定したListenポートでサーバーを起動できる
		log.Fatalf("Failed to serve: %v", err)
	}
}

/*-------------------------------------------------------------------------------------------------------------------
SSL通信
本番環境で用いるサーバー証明書では第三者機関である認証局が発行したものを使用すべき
自己認証局の作成とサーバー証明書の発行 今回はmksertを使用
$ brew install mkcert    //mkcertのインストール
$ mkcert -install    //自己認証局用の証明書と秘密鍵を作成
$ mkcert -CAROOT    //作成された証明書と秘密鍵の場所を確認
---サーバー証明書を発行---
$ mkdir ssl
$ cd ssl
$ mkcert localhost    //localhostには証明書を発行したいドメインを記述
--------------------------------------------------------------------------------------------------------------------*/

