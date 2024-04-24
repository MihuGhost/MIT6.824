# MIT6.824
Distributed Systems

go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrsequential.go wc.so pg*.txt

rm mr-out*
rm wc.so

go run -race mrworker.go wc.so
go run -race mrcoordinator.go pg-*.txt

go build -race -buildmode=plugin -gcflags="all=-N -l"  ../mrapps/wc.go

https://github.com/MihuGhost/MIT6.824.git

插入sh文件，修改写入本地文件名的函数