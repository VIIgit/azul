from abc import ABCMeta, abstractmethod
from collections import Counter
import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Union

from humancellatlas.data.metadata import api

from azul import reject, require
from azul.project.hca.metadata_generator import MetadataGenerator
from azul.transformer import (Accumulator,
                              AggregatingTransformer,
                              Contribution,
                              DistinctAccumulator,
                              Document,
                              EntityReference,
                              FrequencySetAccumulator,
                              GroupingAggregator,
                              SingleValueAccumulator,
                              ListAccumulator,
                              SetAccumulator,
                              SetOfDictAccumulator,
                              SimpleAggregator,
                              SumAccumulator)
from azul.types import JSON

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]
sample_types = api.CellLine, api.Organoid, api.SpecimenFromOrganism
assert Sample.__args__ == sample_types  # since we can't use * in generic types


class Transformer(AggregatingTransformer, metaclass=ABCMeta):

    def get_aggregator(self, entity_type):
        if entity_type == 'files':
            return FileAggregator()
        elif entity_type == 'samples':
            return SampleAggregator()
        elif entity_type == 'specimens':
            return SpecimenAggregator()
        elif entity_type == 'cell_suspensions':
            return CellSuspensionAggregator()
        elif entity_type == 'cell_lines':
            return CellLineAggregator()
        elif entity_type == 'donors':
            return DonorOrganismAggregator()
        elif entity_type == 'organoids':
            return OrganoidAggregator()
        elif entity_type == 'projects':
            return ProjectAggregator()
        elif entity_type == 'protocols':
            return ProtocolAggregator()
        else:
            return super().get_aggregator(entity_type)

    def _find_ancestor_samples(self, entity: api.LinkedEntity, samples: MutableMapping[str, Sample]):
        """
        Populate the `samples` argument with the sample ancestors of the given entity. A sample is any biomaterial
        that is neither a cell suspension nor an ancestor of another sample.
        """
        if isinstance(entity, sample_types):
            samples[str(entity.document_id)] = entity
        else:
            for parent in entity.parents.values():
                self._find_ancestor_samples(parent, samples)

    @classmethod
    def _contact_types(cls) -> Mapping[str, type]:
        return {
            'contact_name': str,
            'corresponding_contributor': bool,
            'email': str,
            'institution': str,
            'laboratory': str,
            'project_role': str
        }

    def _contact(self, p: api.ProjectContact):
        # noinspection PyDeprecation
        return {
            "contact_name": p.contact_name,
            "corresponding_contributor": p.corresponding_contributor,
            "email": p.email,
            "institution": p.institution,
            "laboratory": p.laboratory,
            "project_role": p.project_role
        }

    @classmethod
    def _publication_types(cls) -> Mapping[str, type]:
        return {
            'publication_title': str,
            'publication_url': str
        }

    def _publication(self, p: api.ProjectPublication):
        # noinspection PyDeprecation
        return {
            "publication_title": p.publication_title,
            "publication_url": p.publication_url
        }

    @classmethod
    def _project_types(cls) -> Mapping[str, type]:
        return {
            'project_title': str,
            'project_description': str,
            'project_short_name': str,
            'laboratory': str,
            'institutions': str,
            'contact_names': str,
            'contributors': cls._contact_types(),
            'document_id': str,
            'publication_titles': str,
            'publications': cls._publication_types(),
            'insdc_project_accessions': str,
            'geo_series_accessions': str,
            'array_express_accessions': str,
            'insdc_study_accessions': str,
            '_type': str
        }

    def _project(self, project: api.Project) -> JSON:
        # Store lists of all values of each of these facets to allow facet filtering
        # and term counting on the webservice
        laboratories: Set[str] = set()
        institutions: Set[str] = set()
        contact_names: Set[str] = set()
        publication_titles: Set[str] = set()

        for contributor in project.contributors:
            if contributor.laboratory:
                laboratories.add(contributor.laboratory)
            # noinspection PyDeprecation
            if contributor.contact_name:
                # noinspection PyDeprecation
                contact_names.add(contributor.contact_name)
            if contributor.institution:
                institutions.add(contributor.institution)

        for publication in project.publications:
            # noinspection PyDeprecation
            if publication.publication_title:
                # noinspection PyDeprecation
                publication_titles.add(publication.publication_title)

        return {
            'project_title': project.project_title,
            'project_description': project.project_description,
            'project_short_name': project.project_short_name,
            'laboratory': list(laboratories),
            'institutions': list(institutions),
            'contact_names': list(contact_names),
            'contributors': [self._contact(c) for c in project.contributors],
            'document_id': str(project.document_id),
            'publication_titles': list(publication_titles),
            'publications': [self._publication(p) for p in project.publications],
            'insdc_project_accessions': list(project.insdc_project_accessions),
            'geo_series_accessions': list(project.geo_series_accessions),
            'array_express_accessions': list(project.array_express_accessions),
            'insdc_study_accessions': list(project.insdc_study_accessions),
            '_type': 'project'
        }

    @classmethod
    def _specimen_types(cls) -> Mapping[str, type]:
        return {
            'has_input_biomaterial': str,
            '_source': str,
            'document_id': str,
            'biomaterial_id': str,
            'disease': str,
            'organ': str,
            'organ_part': str,
            'storage_method': str,
            'preservation_method': str,
            '_type': str
        }

    def _specimen(self, specimen: api.SpecimenFromOrganism) -> JSON:
        return {
            'has_input_biomaterial': specimen.has_input_biomaterial,
            '_source': api.schema_names[type(specimen)],
            'document_id': str(specimen.document_id),
            'biomaterial_id': specimen.biomaterial_id,
            'disease': list(specimen.diseases),
            'organ': specimen.organ,
            'organ_part': list(specimen.organ_parts),
            'storage_method': specimen.storage_method,
            'preservation_method': specimen.preservation_method,
            '_type': 'specimen'
        }

    @classmethod
    def _cell_suspension_types(cls) -> Mapping[str, type]:
        return {
            'document_id': str,
            'total_estimated_cells': int,
            'selected_cell_type': str,
            'organ': str,
            'organ_part': str
        }

    def _cell_suspension(self, cell_suspension: api.CellSuspension) -> JSON:
        organs = set()
        organ_parts = set()
        samples: MutableMapping[str, Sample] = dict()
        self._find_ancestor_samples(cell_suspension, samples)
        for sample in samples.values():
            if isinstance(sample, api.SpecimenFromOrganism):
                organs.add(sample.organ)
                organ_parts.update(sample.organ_parts)
            elif isinstance(sample, api.CellLine):
                organs.add(sample.model_organ)
                organ_parts.add(None)
            elif isinstance(sample, api.Organoid):
                organs.add(sample.model_organ)
                organ_parts.add(sample.model_organ_part)
            else:
                assert False
        return {
            'document_id': str(cell_suspension.document_id),
            'total_estimated_cells': cell_suspension.estimated_cell_count,
            'selected_cell_type': list(cell_suspension.selected_cell_types),
            'organ': list(organs),
            'organ_part': list(organ_parts)
        }

    @classmethod
    def _cell_line_types(cls) -> Mapping[str, type]:
        return {
            'document_id': str,
            'biomaterial_id': str,
            'cell_line_type': str,
            'model_organ': str
        }

    def _cell_line(self, cell_line: api.CellLine) -> JSON:
        # noinspection PyDeprecation
        return {
            'document_id': str(cell_line.document_id),
            'biomaterial_id': cell_line.biomaterial_id,
            'cell_line_type': cell_line.cell_line_type,
            'model_organ': cell_line.model_organ
        }

    @classmethod
    def _donor_types(cls) -> Mapping[str, type]:
        return {
            'document_id': str,
            'biomaterial_id': str,
            'biological_sex': str,
            'genus_species': str,
            'diseases': str,
            'organism_age': str,
            'organism_age_unit': str,
            'organism_age_range': dict
        }

    def _donor(self, donor: api.DonorOrganism) -> JSON:
        return {
            'document_id': str(donor.document_id),
            'biomaterial_id': donor.biomaterial_id,
            'biological_sex': donor.sex,
            'genus_species': list(donor.genus_species),
            'diseases': list(donor.diseases),
            'organism_age': donor.organism_age,
            'organism_age_unit': donor.organism_age_unit,
            **(
                {
                    'organism_age_range': {
                        'gte': donor.organism_age_in_seconds.min,
                        'lte': donor.organism_age_in_seconds.max
                    }
                } if donor.organism_age_in_seconds else {
                }
            )
        }

    @classmethod
    def _organoid_types(cls) -> Mapping[str, type]:
        return {
            'document_id': str,
            'biomaterial_id': str,
            'model_organ': str,
            'model_organ_part': str
        }

    def _organoid(self, organoid: api.Organoid) -> JSON:
        return {
            'document_id': str(organoid.document_id),
            'biomaterial_id': organoid.biomaterial_id,
            'model_organ': organoid.model_organ,
            'model_organ_part': organoid.model_organ_part
        }

    @classmethod
    def _file_types(cls) -> Mapping[str, type]:
        return {
            'content-type': str,
            'indexed': bool,
            'name': str,
            'sha256': str,
            'size': int,
            'uuid': api.UUID4,
            'version': str,
            'document_id': str,
            'file_format': str,
            '_type': str,
            'read_index': str,
            'lane_index': int
        }


    def _file(self, file: api.File) -> JSON:
        # noinspection PyDeprecation
        return {
            'content-type': file.manifest_entry.content_type,
            'indexed': file.manifest_entry.indexed,
            'name': file.manifest_entry.name,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'version': file.manifest_entry.version,
            'document_id': str(file.document_id),
            'file_format': file.file_format,
            '_type': 'file',
            **(
                {
                    'read_index': file.read_index,
                    'lane_index': file.lane_index
                } if isinstance(file, api.SequenceFile) else {
                }
            )
        }

    @classmethod
    def _protocol_types(cls) -> Mapping[str, type]:
        return {
            'document_id': str,
            'library_construction_approach': str,
            'instrument_manufacturer_model': str,
            'paired_end': bool,
            'workflow': str,
            'assay_type': str
        }

    def _protocol(self, protocol: api.Protocol) -> JSON:
        protocol_ = {'document_id': protocol.document_id}
        if isinstance(protocol, api.LibraryPreparationProtocol):
            # noinspection PyDeprecation
            protocol_['library_construction_approach'] = protocol.library_construction_approach
        elif isinstance(protocol, api.SequencingProtocol):
            protocol_['instrument_manufacturer_model'] = protocol.instrument_manufacturer_model
            protocol_['paired_end'] = protocol.paired_end
        elif isinstance(protocol, api.AnalysisProtocol):
            protocol_['workflow'] = protocol.protocol_id
        elif isinstance(protocol, api.ImagingProtocol):
            protocol_['assay_type'] = dict(Counter(target.assay_type for target in protocol.target))
        else:
            assert False
        return protocol_

    @classmethod
    def _sample_types(cls) -> Mapping[str, type]:
        return {
            'entity_type': str,
            'effective_organ': str,
            **cls._cell_line_types(),
            **cls._organoid_types(),
            **cls._specimen_types()
        }

    def _sample(self, sample: api.Biomaterial) -> JSON:
        entity_type, sample_ = (
            'cell_lines', self._cell_line(sample)
        ) if isinstance(sample, api.CellLine) else (
            'organoids', self._organoid(sample)
        ) if isinstance(sample, api.Organoid) else (
            'specimens', self._specimen(sample)
        ) if isinstance(sample, api.SpecimenFromOrganism) else (
            require(False, sample), None
        )
        sample_['entity_type'] = entity_type
        assert hasattr(sample, 'organ') != hasattr(sample, 'model_organ')
        sample_['effective_organ'] = sample.organ if hasattr(sample, 'organ') else sample.model_organ
        assert sample_['document_id'] == str(sample.document_id)
        assert sample_['biomaterial_id'] == sample.biomaterial_id
        return sample_

    def _get_project(self, bundle) -> api.Project:
        project, *additional_projects = bundle.projects.values()
        reject(additional_projects, "Azul can currently only handle a single project per bundle")
        assert isinstance(project, api.Project)
        return project

    def _contribution(self, bundle: api.Bundle, contents: JSON, entity_id: api.UUID4, deleted: bool) -> Contribution:
        entity_reference = EntityReference(entity_type=self.entity_type(),
                                           entity_id=str(entity_id))
        return Contribution(entity=entity_reference,
                            version=None,
                            contents=contents,
                            bundle_uuid=str(bundle.uuid),
                            bundle_version=bundle.version,
                            bundle_deleted=deleted)

    @classmethod
    def field_types(cls):
        return {
            'samples': cls._sample_types(),
            'specimens': cls._specimen_types(),
            'cell_suspensions': cls._cell_suspension_types(),
            'cell_lines': cls._cell_line_types(),
            'donors': cls._donor_types(),
            'organoids': cls._organoid_types(),
            'files': cls._file_types(),
            'protocols': cls._protocol_types(),
            'projects': cls._project_types()
        }


class TransformerVisitor(api.EntityVisitor):
    # Entities are tracked by ID to ensure uniqueness if an entity is visited twice while descending the entity DAG
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    cell_suspensions: MutableMapping[api.UUID4, api.CellSuspension]
    cell_lines: MutableMapping[api.UUID4, api.CellLine]
    donors: MutableMapping[api.UUID4, api.DonorOrganism]
    organoids: MutableMapping[api.UUID4, api.Organoid]
    protocols: MutableMapping[api.UUID4, api.Protocol]
    files: MutableMapping[api.UUID4, api.File]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.cell_lines = {}
        self.donors = {}
        self.organoids = {}
        self.protocols = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.SpecimenFromOrganism):
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.CellSuspension):
            self.cell_suspensions[entity.document_id] = entity
        elif isinstance(entity, api.CellLine):
            self.cell_lines[entity.document_id] = entity
        elif isinstance(entity, api.DonorOrganism):
            self.donors[entity.document_id] = entity
        elif isinstance(entity, api.Organoid):
            self.organoids[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for protocol in entity.protocols.values():
                if isinstance(protocol, (api.SequencingProtocol,
                                         api.LibraryPreparationProtocol,
                                         api.AnalysisProtocol,
                                         api.ImagingProtocol)):
                    self.protocols[protocol.document_id] = protocol
        elif isinstance(entity, api.File):
            # noinspection PyDeprecation
            if '.zarr!' in entity.manifest_entry.name and not entity.manifest_entry.name.endswith('.zattrs'):
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                return
            self.files[entity.document_id] = entity


class FileTransformer(Transformer):

    def entity_type(self) -> str:
        return 'files'

    def transform(self,
                  uuid: str,
                  version: str,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Iterable[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        for file in bundle.files.values():
            # noinspection PyDeprecation
            if '.zarr!' in file.manifest_entry.name and not file.manifest_entry.name.endswith('.zattrs'):
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                continue
            visitor = TransformerVisitor()
            file.accept(visitor)
            file.ancestors(visitor)
            samples: MutableMapping[str, Sample] = dict()
            self._find_ancestor_samples(file, samples)
            contents = dict(samples=[self._sample(s) for s in samples.values()],
                            specimens=[self._specimen(s) for s in visitor.specimens.values()],
                            cell_suspensions=[self._cell_suspension(cs) for cs in
                                              visitor.cell_suspensions.values()],
                            cell_lines=[self._cell_line(cl) for cl in visitor.cell_lines.values()],
                            donors=[self._donor(d) for d in visitor.donors.values()],
                            organoids=[self._organoid(o) for o in visitor.organoids.values()],
                            files=[self._file(file)],
                            protocols=[self._protocol(pl) for pl in visitor.protocols.values()],
                            projects=[self._project(project)])
            yield self._contribution(bundle, contents, file.document_id, deleted)


class CellSuspensionTransformer(Transformer):

    def entity_type(self) -> str:
        return 'cell_suspensions'

    def transform(self,
                  uuid: str,
                  version: str,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Iterable[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        for cell_suspension in bundle.biomaterials.values():
            if not isinstance(cell_suspension, api.CellSuspension):
                continue
            samples: MutableMapping[str, Sample] = dict()
            self._find_ancestor_samples(cell_suspension, samples)
            visitor = TransformerVisitor()
            cell_suspension.accept(visitor)
            cell_suspension.ancestors(visitor)
            contents = dict(samples=[self._sample(s) for s in samples.values()],
                            specimens=[self._specimen(s) for s in visitor.specimens.values()],
                            cell_suspensions=[self._cell_suspension(cell_suspension)],
                            cell_lines=[self._cell_line(cl) for cl in visitor.cell_lines.values()],
                            donors=[self._donor(d) for d in visitor.donors.values()],
                            organoids=[self._organoid(o) for o in visitor.organoids.values()],
                            files=[self._file(f) for f in visitor.files.values()],
                            protocols=[self._protocol(pl) for pl in visitor.protocols.values()],
                            projects=[self._project(project)])
            yield self._contribution(bundle, contents, cell_suspension.document_id, deleted)


class SampleTransformer(Transformer):

    def entity_type(self) -> str:
        return 'samples'

    def transform(self,
                  uuid: str,
                  version: str,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        samples: MutableMapping[str, Sample] = dict()
        for file in bundle.files.values():
            self._find_ancestor_samples(file, samples)
        for sample in samples.values():
            visitor = TransformerVisitor()
            sample.accept(visitor)
            sample.ancestors(visitor)
            contents = dict(samples=[self._sample(sample)],
                            specimens=[self._specimen(s) for s in visitor.specimens.values()],
                            cell_suspensions=[self._cell_suspension(cs) for cs in
                                              visitor.cell_suspensions.values()],
                            cell_lines=[self._cell_line(cl) for cl in visitor.cell_lines.values()],
                            donors=[self._donor(d) for d in visitor.donors.values()],
                            organoids=[self._organoid(o) for o in visitor.organoids.values()],
                            files=[self._file(f) for f in visitor.files.values()],
                            protocols=[self._protocol(pl) for pl in visitor.protocols.values()],
                            projects=[self._project(project)])
            yield self._contribution(bundle, contents, sample.document_id, deleted)


class BundleProjectTransformer(Transformer, metaclass=ABCMeta):

    @abstractmethod
    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        raise NotImplementedError()

    def transform(self,
                  uuid: str,
                  version: str,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        # Project entities are not explicitly linked in the graph. The mere presence of project metadata in a bundle
        # indicates that all other entities in that bundle belong to that project. Because of that we can't rely on a
        # visitor to collect the related entities but have to enumerate the explicitly:
        #
        visitor = TransformerVisitor()
        for specimen in bundle.specimens:
            specimen.accept(visitor)
            specimen.ancestors(visitor)
        samples: MutableMapping[str, Sample] = dict()
        for file in bundle.files.values():
            file.accept(visitor)
            file.ancestors(visitor)
            self._find_ancestor_samples(file, samples)
        project = self._get_project(bundle)

        contents = dict(samples=[self._sample(s) for s in samples.values()],
                        specimens=[self._specimen(s) for s in visitor.specimens.values()],
                        cell_suspensions=[self._cell_suspension(cs) for cs in visitor.cell_suspensions.values()],
                        cell_lines=[self._cell_line(cl) for cl in visitor.cell_lines.values()],
                        donors=[self._donor(d) for d in visitor.donors.values()],
                        organoids=[self._organoid(o) for o in visitor.organoids.values()],
                        files=[self._file(f) for f in visitor.files.values()],
                        protocols=[self._protocol(pl) for pl in visitor.protocols.values()],
                        projects=[self._project(project)])

        yield self._contribution(bundle, contents, self._get_entity_id(bundle, project), deleted)


class ProjectTransformer(BundleProjectTransformer):

    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        return project.document_id

    def entity_type(self) -> str:
        return 'projects'


class BundleTransformer(BundleProjectTransformer):

    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        return bundle.uuid

    def get_aggregator(self, entity_type):
        if entity_type in ('files', 'metadata'):
            return None
        else:
            return super().get_aggregator(entity_type)

    def entity_type(self) -> str:
        return 'bundles'

    def transform(self,
                  uuid: str,
                  version: str,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        for contrib in super().transform(uuid, version, deleted, manifest, metadata_files):
            # noinspection PyArgumentList
            if 'project.json' in metadata_files:
                # we can't handle v5 bundles
                metadata = []
            else:
                generator = MetadataGenerator()
                generator.add_bundle(uuid, version, manifest, list(metadata_files.values()))
                metadata = generator.dump()
            contrib.contents['metadata'] = metadata
            yield contrib

    @classmethod
    def field_types(cls):
        return {
            **super().field_types(),
            'metadata': list
        }


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return dict(size=((entity['uuid'], entity['version']), entity['size']),
                    file_format=entity['file_format'],
                    count=((entity['uuid'], entity['version']), 1))

    def _group_keys(self, entity) -> Iterable[Any]:
        return entity['file_format']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'file_format':
            return SingleValueAccumulator()
        elif field in ('size', 'count'):
            return DistinctAccumulator(SumAccumulator(0))
        else:
            return None


class SampleAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class SpecimenAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class CellSuspensionAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return {
            **entity,
            'total_estimated_cells': (entity['document_id'], entity['total_estimated_cells']),
        }

    def _group_keys(self, entity) -> Iterable[Any]:
        return entity['organ']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return DistinctAccumulator(SumAccumulator(0))
        else:
            return SetAccumulator(max_size=100)


class CellLineAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class DonorOrganismAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'organism_age_range':
            return SetOfDictAccumulator(max_size=100)
        else:
            return SetAccumulator(max_size=100)


class OrganoidAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class ProjectAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return ListAccumulator(max_size=100)
        elif field in ('project_description',
                       'contact_names',
                       'contributors',
                       'publication_titles',
                       'publications'):
            return None
        else:
            return SetAccumulator(max_size=100)


class ProtocolAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        elif field == 'assay_type':
            return FrequencySetAccumulator(max_size=100)
        else:
            return SetAccumulator()
