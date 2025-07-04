# toki
Tool for transporting logs from opensearch / elasticsearch to loki

Application is developed in part of Toska hackaton 3.7.2025

![image](https://github.com/user-attachments/assets/994308fe-e381-4498-8782-8195179b428f)
(image is not presenting the current state of the application cuz its not yet parallel)

### Installation & Running

```bash
git clone git:@github.com:tfhuhtal/toki.git
cd toki
```
Install dependencies

```bash
go mod tidy
```
Run the tool

```bash
go run main.go --output=<your_loki_url>/loki/api/v1/push --input=<your_opensearch_url> --index=<your_opensearch_index>
```
