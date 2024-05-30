#
# 调试指令
#

#删除对应文件
rm mr-*
rm wc.so
#重新生成wc.so文件
go build -race -buildmode=plugin ../mrapps/wc.go