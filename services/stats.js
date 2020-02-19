const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    // TODO Compter le nombre d'anomalies par arondissement
    client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs" : {
                "arrondissements" : {
                  "terms" : {
                    "field" : "arrondissement.keyword"
                  }
                }
              }
        }
    }).then(res => callback(res.body.aggregations.arrondissements.buckets.map(element => mettreEnFormeArrondissement(element)))).catch(err => console.error(err));
}

function mettreEnFormeArrondissement(element){
    r = {
        "arrondissement" : element.key,
        "count": element.doc_count,
    }
    return r;
}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    client.search({
    	index : indexName,
    	body : {
		    "aggs" : {
		        "types" : {
		            "terms" : { "field" : "type.keyword", "size" : 5},
		            "aggs" : {
		                "sous_types" : {
		                    "terms" : { "field" : "sous_type.keyword", "size": 5} 
		                }
		            } 
		        }
		    }
		}
    }).then(res => callback( res.body.aggregations.types.buckets.map(element => mettreEnFormeTypes(element)) )).catch(err => console.error(err));
}

function mettreEnFormeTypes(element){
    r = {
        "type" : element.key,
        "count": element.doc_count,
        "sous_types": element.sous_types.buckets
    }
    return r;
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    client.search({
    	index : indexName,
    	body : {
		    "aggs" : {
		        "total": {
                    "composite": {
                        "sources": [
                            {"month": {"terms": {"field": "mois_declaration.keyword"}}},
                            {"year": {"terms": {"field": "annee_declaration.keyword"}}}
                        ],
                        "size": 10000
                    }
                }
		    }
		}
    }).then(res => callback( mettreEnFormeMonthYear(res.body.aggregations.total.buckets) )).catch(err => console.error(err));
}

function mettreEnFormeMonthYear(list){
    l2 = []
    list.sort((a,b) => (a.doc_count < b.doc_count) ? 1 : -1)
    for (var i = 0; i < 10; i++) {
        var month = list[i].key.month+"/"+list[i].key.year;
        (month.length == 6) ? month = "0"+month : month = month;
        l2.push({
            "month": month,
            "count": list[i].doc_count
        })
    }
    return l2;
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    client.search({
    	index : indexName,
    	body : {
            "query": {
                "match":{
                    "type": {
                        "query": "Propreté"
                    }
                }
            },
		    "aggs" : {
		        "Arrondissement": {
                    "terms": {"field": "arrondissement.keyword", "size":3}
                }
		    }
		}
    }).then(res => callback( res.body.aggregations.Arrondissement.buckets.map(element => mettreEnFormePropreteArrondissement(element)) )).catch(err => console.error(err));
    
}

function mettreEnFormePropreteArrondissement(element){
    r = {
        "arrondissement" : element.key,
        "count": element.doc_count,
    }
    return r;
}
