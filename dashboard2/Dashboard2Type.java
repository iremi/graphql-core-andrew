package com.linkedin.datahub.graphql.types.dashboard2.mappers;

import com.linkedin.datahub.graphql.generated.AccessLevel;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard2;
import com.linkedin.datahub.graphql.generated.Dashboard2Info;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Dashboard2EditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class Dashboard2Mapper implements ModelMapper<com.linkedin.dashboard2.Dashboard2, Dashboard2> {

    public static final Dashboard2Mapper INSTANCE = new Dashboard2Mapper();

    public static Dashboard2 map(@Nonnull final com.linkedin.dashboard2.Dashboard2 dashboard2) {
        return INSTANCE.apply(dashboard2);
    }

    @Override
    public Dashboard2 apply(@Nonnull final com.linkedin.dashboard2.Dashboard2 dashboard2) {
        final Dashboard2 result = new Dashboard2();
        result.setUrn(dashboard2.getUrn().toString());
        result.setType(EntityType.DASHBOARD);
        result.setDashboard2Id(dashboard2.getDashboard2Id());
        result.setTool(dashboard2.getTool());
        if (dashboard2.hasInfo()) {
            result.setInfo(mapDashboard2Info(dashboard2.getInfo()));
        }
        if (dashboard2.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(dashboard2.getOwnership()));
        }
        if (dashboard2.hasStatus()) {
            result.setStatus(StatusMapper.map(dashboard2.getStatus()));
        }
        if (dashboard2.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(dashboard2.getGlobalTags()));
        }
        if (dashboard2.hasEditableProperties()) {
            final Dashboard2EditableProperties dashboard2EditableProperties = new Dashboard2EditableProperties();
            dashboard2EditableProperties.setDescription(dashboard2.getEditableProperties().getDescription());
            result.setEditableProperties(dashboard2EditableProperties);
        }
        return result;
    }

    private Dashboard2Info mapDashboard2Info(final com.linkedin.dashboard2.Dashboard2Info info) {
        final Dashboard2Info result = new Dashboard2Info();
        result.setDescription(info.getDescription());
        result.setName(info.getTitle());
        result.setLastRefreshed(info.getLastRefreshed());
        result.setCharts(info.getCharts().stream().map(urn -> {
            final Chart chart = new Chart();
            chart.setUrn(urn.toString());
            return chart;
        }).collect(Collectors.toList()));
        if (info.hasExternalUrl()) {
            // TODO: Migrate to using the External URL field for consistency.
            result.setExternalUrl(info.getDashboard2Url().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        if (info.hasAccess()) {
            result.setAccess(AccessLevel.valueOf(info.getAccess().toString()));
        }
        result.setLastModified(AuditStampMapper.map(info.getLastModified().getLastModified()));
        result.setCreated(AuditStampMapper.map(info.getLastModified().getCreated()));
        if (info.getLastModified().hasDeleted()) {
            result.setDeleted(AuditStampMapper.map(info.getLastModified().getDeleted()));
        }
        return result;
    }
}

