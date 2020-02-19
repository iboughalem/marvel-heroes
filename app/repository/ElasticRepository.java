package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        int from = (page - 1)*size;
        return wsClient.url(elasticConfiguration.uri + "/_search")
                .post(Json.parse("{\n" +
                        "    \"query\": {\n" +
                        "      \"multi_match\" : {\n" +
                        "        \"query\":    \""+ input + "\",\n" +
                        "        \"fields\": [ \"name^4\", \"aliases^3\", \"secret_identities^3\", \"description^2\", \"partners\" ]\n" +
                        "      }\n" +
                        "    },\n" +
                        "     \"from\": " + from + "," +
                        "      \"size\": " + size +
                        "  }"))
                .thenApply(response -> {
                    JsonNode responseObj = response.asJson().get("hits");
                    Iterator<JsonNode> hits = responseObj.withArray("hits").elements();
                    ArrayList<SearchedHero> heroes = new ArrayList<>();
                    while (hits.hasNext()){
                        JsonNode hero = hits.next().get("_source");
                        SearchedHero searchedHero = SearchedHero.fromJson(hero);
                        heroes.add(searchedHero);
                    }
                    int numHeroes = responseObj.get("total").get("value").asInt();
                    int total = (int) Math.ceil((double) numHeroes/size);
                    return new PaginatedResults<>(numHeroes, page, total, heroes);
                });
    }

    /*La suggestion doit tenir compte également des alias et identités secrètes des Héros*/
    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(
                        "{\n" +
                                "    \"suggest\": {\n" +
                                "      \"suggest-alias-secret\" : {\n" +
                                "        \"prefix\" : \""+ input +"\",\n" +
                                "        \"completion\" : {\n" +
                                "          \"field\" : \"suggest\"\n" +
                                "        }\n" +
                                "      }\n" +
                                "      }\n" +
                                "  }"
                ))
                .thenApply(response -> {
                    Iterator<JsonNode> hits = response.asJson()
                            .get("suggest").withArray("suggest-alias-secret").get(0).withArray("options").iterator();
                    ArrayList<SearchedHero> heroes = new ArrayList<>();
                    while (hits.hasNext()){
                        JsonNode hero = hits.next().get("_source");
                        SearchedHero searchedHero = SearchedHero.fromJson(hero);
                        heroes.add(searchedHero);
                    }
                    return heroes;
                });
    }
}
