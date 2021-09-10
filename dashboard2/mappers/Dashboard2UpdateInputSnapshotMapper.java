package com.linkedin.datahub.graphql.types.dashboard2.mappers;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;

import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.Dashboard2Urn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard2.EditableDashboard2Properties;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.generated.Dashboard2UpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.InputModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagAssociationUpdateMapper;
import com.linkedin.metadata.aspect.Dashboard2Aspect;
import com.linkedin.metadata.aspect.Dashboard2AspectArray;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.Dashboard2Snapshot;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class Dashboard2UpdateInputSnapshotMapper implements InputModelMapper<Dashboard2UpdateInput, Dashboard2Snapshot, Urn> {
    public static final Dashboard2UpdateInputSnapshotMapper INSTANCE = new Dashboard2UpdateInputSnapshotMapper();

    public static Dashboard2Snapshot map(@Nonnull final Dashboard2UpdateInput dashboard2UpdateInput,
                                @Nonnull final Urn actor) {
        return INSTANCE.apply(dashboard2UpdateInput, actor);
    }

    @Override
    public Dashboard2Snapshot apply(@Nonnull final Dashboard2UpdateInput dashboard2UpdateInput,
                           @Nonnull final Urn actor) {
        final Dashboard2Snapshot result = new Dashboard2Snapshot();
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        try {
            result.setUrn(Dashboard2Urn.createFromString(dashboard2UpdateInput.getUrn()));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                String.format("Failed to validate provided urn with value %s", dashboard2UpdateInput.getUrn()));
        }

        final Dashboard2AspectArray aspects = new Dashboard2AspectArray();

        if (dashboard2UpdateInput.getOwnership() != null) {
            aspects.add(Dashboard2Aspect.create(OwnershipUpdateMapper.map(dashboard2UpdateInput.getOwnership(), actor)));
        }

        if (dashboard2UpdateInput.getGlobalTags() != null) {
            final GlobalTags globalTags = new GlobalTags();
            globalTags.setTags(
                new TagAssociationArray(
                    dashboard2UpdateInput.getGlobalTags().getTags().stream().map(
                        element -> TagAssociationUpdateMapper.map(element)
                    ).collect(Collectors.toList())
                )
            );
            aspects.add(Dashboard2Aspect.create(globalTags));
        }

        if (dashboard2UpdateInput.getEditableProperties() != null) {
            final EditableDashboard2Properties editableDashboard2Properties = new EditableDashboard2Properties();
            editableDashboard2Properties.setDescription(dashboard2UpdateInput.getEditableProperties().getDescription());
            if (!editableDashboard2Properties.hasCreated()) {
                editableDashboard2Properties.setCreated(auditStamp);
            }
            editableDashboard2Properties.setLastModified(auditStamp);
            aspects.add(ModelUtils.newAspectUnion(Dashboard2Aspect.class, editableDashboard2Properties));
        }

        result.setAspects(aspects);

        return result;
    }

}
