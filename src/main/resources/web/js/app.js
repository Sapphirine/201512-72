var app = angular.module('app', ['ngTouch', 'ui.grid']);

app.factory('_', ['$window',
    function($window) {
        return $window._;
    }
]);

app.directive('uiGridRow', function ($animate, $timeout) {
    return {
        priority: -1,
        link: function ($scope, $elm, $attrs) {
         $scope.$watch('row.entity', function (n, o) {
           if ($scope.row.isNew) {
             $elm.addClass('new-row');

             $timeout(function () {
               $animate.removeClass($elm, 'new-row');
             });

             $scope.row.isNew = false;
           }
         });
        }
    };
});