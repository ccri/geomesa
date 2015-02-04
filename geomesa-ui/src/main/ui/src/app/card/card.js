angular.module('geomesa.card', [])

    .directive('cards', [function () {
        return {
            restrict: 'E',
            scope: {
                selectedPoint: '='
            },
            templateUrl: 'card/card.tpl.html',
            link: function (scope, element, attrs) {
                scope.$watch('selectedPoint', function (p) {
                    if (p.lat && p.lng) {
                        console.log(p);
                    }
                }, true);
            }
        };
    }]);