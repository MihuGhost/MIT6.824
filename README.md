# MIT6.824
Distributed Systems
rm mr-out*
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrsequential.go wc.so pg*.txt
go run -race mrworker.go wc.so
go run -race mrcoordinator.go pg-*.txt

go build -race -buildmode=plugin -gcflags="all=-N -l"  ../mrapps/wc.go
rm wc.so

https://github.com/MihuGhost/MIT6.824.git