const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    // TODO Compter le nombre d'anomalies entre deux dates
    client.count({
        index: indexName,
        body:{
            query: {
                range : {
                    "@timestamp" : {
                        "gte" : from,
                        "lt" : to
                    }
                }
            }
        }
    }).then(res => callback({
        count: res.body.count
    })).catch(err => console.error(err));
    
}

exports.countAround = (client, lat, lon, radius, callback) => {
    // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
    client.count({
        index: indexName,
        body:{
            query: {
                bool: {
                    filter : {
                        geo_distance : {
                            distance : radius,
                            "location" : {
                                "lat" : lat,
                                "lon" : lon
                            }
                        }
                    }
                }
                
            }
        }
    }).then(res => callback({
        count: res.body.count
    })).catch(err => console.error(err));
}