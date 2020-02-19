var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

const MAX_CHUNK_SIZE = 5000;

async function run() {
    const client = await MongoClient.connect(mongoUrl);

    const db = client.db(dbName);

    const heroCollection = db.collection(collectionName);

    const heroes = [];

    fs.createReadStream('./all-heroes.csv')
        .pipe(csv())
        .on('data', async data => {
            heroes.push({
                id: data.id,
                name: data.name,
                imageUrl: data.imageUrl,
                backgroundImageUrl: data.backgroundImageUrl,
                externalLink: data.externalLink,
                description: data.description,
                identity: {
                    secretIdentities: data.secretIdentities != null  ? data.secretIdentities.split(',') : [],
                    birthPlace: data.birthPlace,
                    occupation: data.occupation,
                    aliases: data.aliases != null ? data.aliases.split(',') : [],
                    alignment: data.alignment,
                    firstAppearance: data.firstAppearance,
                    yearAppearance: data.yearAppearance,
                    universe: data.universe,
                },
                appearance: {
                    gender: data.gender,
                    type: data.type,
                    race: data.race,
                    height: data.height,
                    weight: data.weight,
                    eyeColor: data.eyeColor,
                    hairColor: data.hairColor,
                },

                teams: data.teams != null ? data.teams.split(',') : [],
                powers:  data.powers != null ? data.powers.split(',') : [],
                partners:  data.partners != null ? data.partners.split(',') : [],
                skills : {
                    intelligence: data.intelligence,
                    strength: !isNaN(data.strength) && data.strength.length !== 0 ? parseFloat(data.strength) : 0,
                    speed: !isNaN(data.speed) && data.speed.length !== 0 ? parseFloat(data.speed) : 0,
                    durability: !isNaN(data.durability) && data.durability.length !== 0 ? parseFloat(data.durability) : 0,
                    combat: !isNaN(data.combat) && data.combat.length !== 0 ? parseFloat(data.combat) : 0,
                    power: !isNaN(data.power) && data.power.length !== 0 ? parseFloat(data.power) : 0,
                },
                creators: data.creators != null ? data.creators.split(',') : []
            });

            //console.log('DATAAA -----------------------', typeof heroes);

            if (heroes.length > MAX_CHUNK_SIZE) {
                await heroCollection.insertMany();
                heroes = [];
            }
        })
        .on('end', async () => {
            try {
                await heroCollection.insertMany();
            } catch (err) {
                console.trace(err);
            } finally {
                client.close();
                console.log('Terminated!');
            }
        })
        .on('error', (err) => {
            console.trace(err);
        });
}

run().catch(console.error);
