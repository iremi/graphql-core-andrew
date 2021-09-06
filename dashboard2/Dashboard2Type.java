package com.linkedin.datahub.graphql.types.dashboard2;

import com.linkedin.common.urn.CorpuserUrn;

import com.linkedin.common.urn.Dashboard2Urn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Dashboard2;
import com.linkedin.datahub.graphql.generated.Dashboard2UpdateInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dashboard2.mappers.Dashboard2SnapshotMapper;
import com.linkedin.datahub.graphql.types.dashboard2.mappers.Dashboard2UpdateInputSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.configs.Dashboard2SearchConfig;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.snapshot.Dashboard2Snapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;

import graphql.execution.DataFetcherResult;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class Dashboard2Type implements SearchableEntityType<Dashboard2>, BrowsableEntityType<Dashboard2>, MutableType<Dashboard2UpdateInput> {

    private final EntityClient _dashboards2Client;
    private static final Dashboard2SearchConfig DASHBOARDS_SEARCH_CONFIG = new Dashboard2SearchConfig();

    public Dashboard2Type(final EntityClient dashboards2Client) {
        _dashboards2Client = dashboards2Client;
    }

    @Override
    public Class<Dashboard2UpdateInput> inputClass() {
        return Dashboard2UpdateInput.class;
    }

    @Override
    public EntityType type() {
        return EntityType.DASHBOARD;
    }

    @Override
    public Class<Dashboard2> objectClass() {
        return Dashboard2.class;
    }

    @Override
    public List<DataFetcherResult<Dashboard2>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
        final List<Dashboard2Urn> dashboard2Urns = urns.stream()
                .map(this::getDashboard2Urn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> dashboard2Map = _dashboards2Client.batchGet(dashboard2Urns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<Entity> gmsResults = new ArrayList<>();
            for (Dashboard2Urn urn : dashboard2Urns) {
                gmsResults.add(dashboard2Map.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsDashboard2 -> gmsDashboard2 == null ? null
                            : DataFetcherResult.<Dashboard2>newResult()
                            .data(Dashboard2SnapshotMapper.map(gmsDashboard2.getValue().getDashboard2Snapshot()))
                            .localContext(AspectExtractor.extractAspects(gmsDashboard2.getValue().getDashboard2Snapshot()))
                            .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Dashboards2", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, DASHBOARDS_SEARCH_CONFIG.getFacetFields());
        final SearchResult searchResult = _dashboards2Client.search("dashboard2", query, facetFilters, start, count);
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, DASHBOARDS_SEARCH_CONFIG.getFacetFields());
        final AutoCompleteResult result = _dashboards2Client.autoComplete("dashboard2", query, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start, int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, DASHBOARDS_SEARCH_CONFIG.getFacetFields());
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _dashboards2Client.browse(
                "dashboard2",
                pathStr,
                facetFilters,
                start,
                count);
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull QueryContext context) throws Exception {
        final StringArray result = _dashboards2Client.getBrowsePaths(getDashboard2Urn(urn));
        return BrowsePathsMapper.map(result);
    }

    private com.linkedin.common.urn.Dashboard2Urn getDashboard2Urn(String urnStr) {
        try {
            return Dashboard2Urn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve dashboard2 with urn %s, invalid urn", urnStr));
        }
    }

    @Override
    public Dashboard2 update(@Nonnull Dashboard2UpdateInput input, @Nonnull QueryContext context) throws Exception {

        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final Dashboard2Snapshot partialDashboard2 = Dashboard2UpdateInputSnapshotMapper.map(input, actor);
        final Snapshot snapshot = Snapshot.create(partialDashboard2);

        try {
            _dashboards2Client.update(new com.linkedin.entity.Entity().setValue(snapshot));
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context).getData();
    }
}
