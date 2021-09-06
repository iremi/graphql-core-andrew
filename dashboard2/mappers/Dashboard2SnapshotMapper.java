package com.linkedin.datahub.graphql.types.dashboard2.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.dashboard2.EditableDashboard2Properties;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.Dashboard2Snapshot;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;


public class Dashboard22SnapshotMapper implements ModelMapper<Dashboard2Snapshot, Dashboard2> {

    public static final Dashboard22SnapshotMapper INSTANCE = new Dashboard22SnapshotMapper();

    public static Dashboard2 map(@Nonnull final Dashboard2Snapshot dashboard2) {
        return INSTANCE.apply(dashboard2);
    }

    @Override
    public Dashboard2 apply(@Nonnull final Dashboard2Snapshot dashboard2) {
        final Dashboard2 result = new Dashboard2();
        result.setUrn(dashboard2.getUrn().toString());
        result.setType(EntityType.DASHBOARD);
        result.setDashboard2Id(dashboard2.getUrn().getDashboard2IdEntity());
        result.setTool(dashboard2.getUrn().getDashboard2ToolEntity());

        ModelUtils.getAspectsFromSnapshot(dashboard2).forEach(aspect -> {
            if (aspect instanceof com.linkedin.dashboard2.Dashboard2Info) {
                com.linkedin.dashboard2.Dashboard2Info info = com.linkedin.dashboard2.Dashboard2Info.class.cast(aspect);
                result.setInfo(mapDashboard2Info(info));
            } else if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                result.setStatus(StatusMapper.map(status));
            } else if (aspect instanceof GlobalTags) {
                result.setGlobalTags(GlobalTagsMapper.map(GlobalTags.class.cast(aspect)));
            } else if (aspect instanceof EditableDashboard2Properties) {
                final Dashboard2EditableProperties dashboard2EditableProperties = new Dashboard2EditableProperties();
                dashboard2EditableProperties.setDescription(((EditableDashboard2Properties) aspect).getDescription());
                result.setEditableProperties(dashboard2EditableProperties);
            }
        });
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
            result.setExternalUrl(info.getExternalUrl().toString());
        } else if (info.hasDashboard2Url()) {
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
