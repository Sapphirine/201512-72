var updateGridData = function (eventData, gridData) {
     var entries = angular.fromJson(eventData);

     var newEntries = _.filter(entries, function(entry) {
        var matches = _.where(gridData.data, {accountId: entry.accountId, productId: entry.productId});
        _.each(matches, function(match) {
            if(_.has(entry, "quantity")) match.quantity = entry.quantity;
            if(_.has(entry, "price")) match.price = entry.price;
        });
        return _.isEmpty(matches);
     });

     gridData.data = gridData.data.concat(newEntries);

     return gridData;
 };