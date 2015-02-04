angular.module('geomesa.map', [])

    .directive('geomesaMap', [function () {
        return {
            restrict: 'E',
            scope: {
                map: '=?',
                api: '=',
                selectedPoint: '='
            },
            link: function (scope, element, attrs) {
                var baseLayer = L.tileLayer.provider('Stamen.TonerLite'),
                    wmsLayer = L.tileLayer.wms("http://dgeo:8080/geoserver/sf/wms", {
                        layers: 'sf:bugsites',
                        format: 'image/png',
                        transparent: true
                    });

                scope.map = L.map(element[0], {
                    center: L.latLng(40.279957,-74.73862),
                    zoom: 8,
                    maxZoom: 18,
                    minZoom: 3,
                    attributionControl: false,
                    layers: [baseLayer, wmsLayer]
                });

                scope.map.on('click', function (evt) {
                    scope.$apply(function () {
                        scope.selectedPoint = {
                            lat: evt.latlng.lat,
                            lng: evt.latlng.lng
                        };
                    });
                });

                scope.api = {
                    applyCQL: function (cql) {
                        console.log(cql);
                    }
                };

            }
        };
    }]);