# How to setup Pinot UI for development 

1. Make sure pinot backend is running on port 9000. Follow [this guide](https://github.com/apache/pinot?tab=readme-ov-file#building-pinot) for the same.
2. Navigate to ui source code folder 
```shell
cd pinot-controller/src/main/resources
```
3. Install Required Packages. Make sure you are using node v14 or more specifially v14.18.1
```shell
npm install 
```
4. Start the Development Server
```shell
npm run dev
```

5. App should be running on [http://localhost:8080](http://localhost:8080)
