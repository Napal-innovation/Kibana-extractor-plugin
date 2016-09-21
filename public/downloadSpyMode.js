define(function(require) {
  function DownloadSpyProvider(Notifier, $filter, $rootScope, config, Private, $http, $q) {
    const _ = require('lodash');
    const PER_PAGE_DEFAULT = 10;

    const downloadLinkFn = function downloadLinkFn($scope, config) {
        $scope.preparing = true
        $scope.failed = false
        $scope.result = null
        $scope.$bind('req', 'searchSource.history[searchSource.history.length - 1]')
        $scope.$watchMulti([
            'req',
            'req.started',
            'req.stopped',
            'searchSource'
        ], function() {
            if (!$scope.searchSource || !$scope.req || !$scope.req.stopped) {
                return
            }

            var reqdata = $scope.req.fetchParams.body

            const canceller = $q.defer();
            const cancel = (reason) => {
                canceller.resolve(reason);
            }

            var req_promise = $http({
                method: 'POST',
                url: '/api/extractor/index',
                data: reqdata,
                timeout: canceller.promise,
            }).then(
                (response) => {
                    $scope.result = response.data
                    $scope.preparing = false
                },
                (response) => {
                    $scope.preparing = false
                    $scope.failed = true
                }
            )

            $scope.$on('$destroy', function() {
                cancel()
            });
        })
    }

    return {
        name: 'download',
        display: 'Extract',
        order: 5,
        template: require('plugins/kibana-extractor-plugin/downloadSpyMode.html'),
        link: downloadLinkFn,
    };
  }

  require('ui/registry/spy_modes').register(DownloadSpyProvider);
});
