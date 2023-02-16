#!/usr/bin/env node
import { createClient, SchemaFieldTypes, AggregateGroupByReducers, AggregateSteps } from 'redis';
import bikes from './data/bike_data.json' assert { type: 'json' };

async function bikesExample() {
  try {
    const client = createClient();
    await client.connect();

    // a single bike to demonstrate the basics
    var bike1 = {
      "model": "Hyperion",
      "brand": "Velorim",
      "price": 844,
      "type": "Enduro bikes",
      "specs": {
        "material": "full-carbon",
        "weight": 8.7
      },
      "description": "This is a mid-travel trail slayer that is a fantastic daily driver or one bike quiver. At this price point, you get a Shimano 105 hydraulic groupset with a RS510 crank set. The wheels have had a slight upgrade for 2022, so you\"re now getting DT Swiss R470 rims with the Formula hubs. Put it all together and you get a bike that helps redefine what can be done for this price."
    }

    await Promise.all([
      client.json.set('bikes:1', '$', bike1)
    ]);

    // retrive bikes:1 model and specs.material
    let results = await client.json.get('bikes:1', {
      path: [
        '$.model',
        '$.specs.material'
      ]
    });
    console.log(results);

    await client.json.set('bikes:1', '$.model', 'Hyperion1');
    console.log(await client.json.get('bikes:1', { path: '$.model' }));

    // load the entire set of bike data
    bikes.data.forEach(async (bike, index) => {
      client.json.set(`bikes:${index}`, '$', bike);
    });

    // define the fields for a bike
    const bike_schema = {
      '$.model': { type: SchemaFieldTypes.TEXT, SORTABLE: true, AS: 'model' },
      '$.brand': { type: SchemaFieldTypes.TEXT, SORTABLE: true, AS: 'brand' },
      '$.price': { type: SchemaFieldTypes.NUMERIC, SORTABLE: true, AS: 'price' },
      '$.type': { type: SchemaFieldTypes.TAG, AS: 'type' },
      '$.specs.material': { type: SchemaFieldTypes.TAG, AS: 'material' },
      '$.specs.weight': { type: SchemaFieldTypes.NUMERIC, SORTABLE: true, AS: 'weight' },
      '$.description': { type: SchemaFieldTypes.TEXT, SORTABLE: 'UNF', AS: 'description' }
    }

    // create the index
    try {
      await client.ft.create('idx:bikes', bike_schema, {
        // define index information
        ON: 'JSON',
        PREFIX: 'bikes:'
      });
    } catch (e) {
      if (e.message === 'Index already exists') {
        console.log('Index exists already, skipped creation.');
      } else {
        // Something went wrong, perhaps RediSearch isn't installed...
        console.error(e);
        process.exit(1);
      }
    }

    // some queries by type of bike
    console.log(JSON.stringify(await client.ft.search('idx:bikes', '@type:{eBikes}'), null, 2));
    console.log(JSON.stringify(await client.ft.search('idx:bikes', '@type:{eBikes} @price:[200 1200]', { RETURN: ['model', 'type', 'price'] }), null, 2));
    console.log(JSON.stringify(await client.ft.search('idx:bikes', 'mudguards @type:{Mountain bikes} @price:[1000 3000]', { RETURN: ['model', 'type', 'price', 'description'] }), null, 2));

    // aggregate request to get the number of bikes of each type that are priced between 1000 and 3000
    var agg_price = await client.ft.aggregate('idx:bikes', '@price:[1000 3000]', {
      STEPS: [
        {
          type: AggregateSteps.GROUPBY,
          properties: ['@type'],
          REDUCE: [{ type: AggregateGroupByReducers.COUNT, property: 'type' }]
        }, {
          type: AggregateSteps.SORTBY,
          BY: { BY: '@type', DIRECTION: 'DESC' }
        }
      ]
    });

    console.log('\n>>>> Number of bikes of each type that are priced between 1000 and 3000:');
    agg_price.results.forEach(async (result) => {
      console.log(`type:${result.type}, count:${result.__generated_aliascount}`);
    });

    // aggregate request to get average weight of bikes in each category (type)
    var agg_weight = await client.ft.aggregate('idx:bikes', '*', {
      STEPS: [
        {
          type: AggregateSteps.GROUPBY,
          properties: ['@type'],
          REDUCE: [{ type: AggregateGroupByReducers.AVG, property: 'weight', AS: 'average_bike_weight' }]
        }, {
          type: AggregateSteps.SORTBY,
          BY: { BY: '@average_bike_weight', DIRECTION: 'DESC' }
        }
      ]
    });

    console.log('\n>>>> Get average weight of bikes in each category (type):');
    agg_weight.results.forEach(async (result) => {
      console.log(`type:${result.type}, count:${result.average_bike_weight}`);
    });

    // aggregate request to get the number of bikes per weight range using the floor() function to create weight groups
    var agg_weight_plus = await client.ft.aggregate('idx:bikes', '*', {
      STEPS: [
        {
          type: AggregateSteps.APPLY,
          expression: 'floor(@weight)',
          AS: 'weight'
        },
        {
          type: AggregateSteps.GROUPBY,
          properties: ['@weight'],
          REDUCE: [{ type: AggregateGroupByReducers.COUNT, property: 'weight', AS: 'count' }]
        }, {
          type: AggregateSteps.SORTBY,
          BY: { BY: '@weight', DIRECTION: 'DESC' }
        }
      ]
    });

    console.log('\n>>>> Number of bikes per weight range using the floor() function to create weight groups:');
    agg_weight_plus.results.forEach(async (result) => {
      console.log(`weight:${result.weight}, count:${result.count}`);
    });

    await client.quit();
  } catch (e) {
    console.error(e);
  }
}

bikesExample();
