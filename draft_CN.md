# 这里记录设计思路

# 整体设计
- ![](./doc/architecture.png)



# 遇到的问题和解决方法
##

## 1. batch mode下, 怎么确保 提交的sql "有序执行"

比如在 dbt 中有 modelA 依赖 modelB, 那么
