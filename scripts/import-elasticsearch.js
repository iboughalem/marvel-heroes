const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const esClient = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'

const MAX_CHUNK_SIZE = 5000;

async function run () {
    
    // Création de l'indice
    esClient.indices.create({ index: heroesIndexName,
        body: {
            mappings: {
                properties: {
                    suggest: {"type": "completion"}
                }
            }
        }

    });
    console.log(`Index ${heroesIndexName} cree`)

    let heroes = [];

    // Read CSV file
    fs.createReadStream('all-heroes.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            heroes.push({
                ...data,
                suggest: [
                    {
                        input: data.name,
                        weight: 10
                    },
                    {
                        input: data.secretIdentities,
                        weight: 5
                    },
                    {
                        input: data.aliases,
                        weight: 5
                    }
                ]
            });

            if (heroes.length > MAX_CHUNK_SIZE) {
                esClient.bulk(createBulkInsertQuery(heroes));
                heroes = [];
            }

        })
        .on('end', () => {
            try {
                esClient.bulk(createBulkInsertQuery(heroes));
            } catch (err) {
                console.trace(err);
            } finally {
                esClient.close();
            }
            console.log('Terminated!');
        })
        .on('error', (err) => {
            console.trace(err);
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(heroes) {
    const body = heroes.reduce((acc, hero) => {
      acc.push({
        index: { _index: heroesIndexName, _type: "_doc", _id: hero.id }
      });
      acc.push(hero);
      return acc;
    }, []);
  
    return { body };
}



run().catch(console.error);
