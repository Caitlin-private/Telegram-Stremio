"""Stable library IDs and normalized read models for addon consumers."""
from copy import deepcopy

from Backend.helper.ingestion_rules import nonnegative_number

SERIES_PREFIX = 'tgcaitlin-'


def series_id(imdb_id):
    return SERIES_PREFIX + imdb_id if imdb_id else ''


def original_id(value):
    return value.removeprefix(SERIES_PREFIX)


def merge_media_documents(documents):
    """Merge already-authorized copies without mutating persisted documents."""
    if not documents:
        return None
    result = deepcopy(documents[0])
    is_tv = result.get('media_type') in ('tv', 'series') or result.get('type') == 'tv'
    seasons = {}
    movie_streams = {}
    for doc in documents:
        for quality in doc.get('telegram') or []:
            if quality.get('id'):
                movie_streams.setdefault(quality['id'], deepcopy(quality))
        for season in doc.get('seasons') or []:
            sn = nonnegative_number(season.get('season_number'))
            if sn is None:
                continue
            episodes = seasons.setdefault(sn, {})
            for episode in season.get('episodes') or []:
                en = nonnegative_number(episode.get('episode_number'))
                if en is None:
                    en = nonnegative_number(episode.get('absolute_episode'))
                if en is None:
                    continue
                target = episodes.setdefault(en, deepcopy(episode))
                target['episode_number'] = en
                target['absolute_episode'] = nonnegative_number(target.get('absolute_episode'))
                streams = {q['id']: q for q in target.get('telegram') or [] if q.get('id')}
                for q in episode.get('telegram') or []:
                    if q.get('id'):
                        streams.setdefault(q['id'], deepcopy(q))
                target['telegram'] = list(streams.values())
    if is_tv:
        result.pop('telegram', None)
        result['seasons'] = [
            {'season_number': sn, 'episodes': [episodes[e] for e in sorted(episodes)]}
            for sn, episodes in sorted(seasons.items())
        ]
    else:
        result['telegram'] = list(movie_streams.values())
    return result


def select_media(media, season_number=None, episode_number=None, absolute_episode=None):
    if not media:
        return None
    if season_number is None and episode_number is None and absolute_episode is None:
        return media
    for season in media.get('seasons') or []:
        sn = season['season_number']
        if season_number is not None and sn != season_number:
            continue
        if episode_number is None and absolute_episode is None:
            return dict(season, type='tv', imdb_id=media.get('imdb_id'), db_index=media.get('db_index'))
        for episode in season.get('episodes') or []:
            if absolute_episode is not None:
                matches = episode.get('absolute_episode') == absolute_episode
                matches = matches or (episode.get('absolute_episode') is None and sn == 1 and episode['episode_number'] == absolute_episode)
            else:
                matches = episode['episode_number'] == episode_number
            if matches:
                return dict(episode, type='tv', imdb_id=media.get('imdb_id'), kitsu_id=media.get('kitsu_id'),
                            title=media.get('title'), is_anime=media.get('is_anime'), season_number=sn,
                            backdrop=episode.get('episode_backdrop'), db_index=media.get('db_index'))
    return None
