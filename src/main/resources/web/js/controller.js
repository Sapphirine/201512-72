angular.module("app")
.controller('MainCtrl', ['$scope', '$timeout', '_', function ($scope, $timeout) {
    var socket;
    if (window.WebSocket) {
        socket = new WebSocket("ws://192.168.0.109:8080/myapp");
        socket.onmessage = function (event) {
            console.log("Received data ", event.data);
            $scope.gridData = updateGridData(event.data, $scope.gridData);
            $scope.refresh = true;
            $timeout(function() {
              $scope.refresh = false;
            }, 0);
        }
        socket.onopen = function (event) {
            alert("Web Socket opened!");
        };
        socket.onclose = function (event) {
            alert("Web Socket closed.");
        };
    } else {
        alert("Your browser does not support Websockets. (Use Chrome)");
    }

    var myData = [];
    $scope.refresh = false;
    $scope.gridData = {data: myData};
    $scope.gridData.columnDefs = [{ field: 'accountId', groupable: true },
                                  { field: 'accountName' },
                                  { field: 'productId' },
                                  { field: 'product' },
                                  { field: 'quantity', cellClass: 'cell-blue' },
                                  { field: 'price', cellClass: 'cell-green' },
                                  { field: 'sentiment', cellClass: 'cell-green' }];
    $scope.gridData.enableFiltering = true;
    $scope.gridData.enableRowSelection = true;

    $scope.gridData.onRegisterApi = function( gridApi ) {
        gridApi.grid.registerRowBuilder(function (row, gridOptions) {
            row.isNew = true;
        });
    };
}]);