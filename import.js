const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    await client.indices.delete({
      index: indexName,
      ignore_unavailable: true
    });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    await client.indices.create({ 
      index: indexName, 
      body: {
      mappings: {
        properties: {
          "@timestamp": {
            "type": "date",
            "format": "yyyy-MM-dd"
          },
          "location": {
            "type": "geo_point"
          }
        }
      }
    } 
    }, (err, resp) => {
        if (err) console.trace(err.message);
    });
    let reports = [];
    const BULK_SIZE = 50000;
    let totalDocumentsCreated = 0;
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))

        .on('data', (data) => {
            // TODO ici on récupère les lignes du CSV ...
            reports.push({
              index: {
                _id: data["OBJECTID"]
              }
            });
            
            reports.push({
                "@timestamp" : data["DATEDECL"],
                object_id : data["OBJECTID"],
                annee_declaration : data["ANNEE DECLARATION"],
                mois_declaration : data["MOIS DECLARATION"],
                type : data["TYPE"],
                sous_type : data["SOUSTYPE"],
                code_postal : data["CODE_POSTAL"],
                ville : data["VILLE"],
                arrondissement : data["ARRONDISSEMENT"],
                prefixe : data["PREFIXE"],
                intervenant : data["INTERVENANT"],
                conseil_de_quartier : data["CONSEIL DE QUARTIER"],
                location : data["geo_point_2d"]
            });
            
            if (reports.length >= BULK_SIZE) {
              client.bulk({
                index: indexName,
                body: reports
              }).then(response => {
                totalDocumentsCreated += response.body.items.length;
                console.log(totalDocumentsCreated + ' documents envoyés');
              }).catch(err => console.error(err));
              reports = [];
            }
        })
        .on('end', () => {
            // TODO il y a peut être des choses à faire à la fin aussi !

          if (reports.length != 0) {
            client.bulk({
              index: indexName,
              body: reports
            }).then(response => {
              totalDocumentsCreated += response.body.items.length;
              console.log(totalDocumentsCreated + ' documents envoyés');
            }).catch(err => console.error(err));
          };
          client.close();
        });
        
}

run().catch(console.error);
