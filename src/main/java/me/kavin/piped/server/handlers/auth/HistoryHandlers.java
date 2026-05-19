package me.kavin.piped.server.handlers.auth;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import me.kavin.piped.utils.DatabaseHelper;
import me.kavin.piped.utils.DatabaseSessionFactory;
import me.kavin.piped.utils.ExceptionHandler;
import me.kavin.piped.utils.obj.db.User;
import me.kavin.piped.utils.obj.db.UserHistory;
import me.kavin.piped.utils.resp.AcceptedResponse;
import me.kavin.piped.utils.resp.AuthenticationFailureResponse;
import me.kavin.piped.utils.resp.InvalidRequestResponse;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.StatelessSession;

import java.io.IOException;
import java.util.List;

import static me.kavin.piped.consts.Constants.mapper;

public class HistoryHandlers {

    public static byte[] getHistoryResponse(String session, Integer limit, Long before) throws IOException {
        if (StringUtils.isBlank(session))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("session required"));

        User user = DatabaseHelper.getUserFromSession(session);
        if (user == null)
            ExceptionHandler.throwErrorResponse(new AuthenticationFailureResponse());

        int max = (limit != null && limit > 0 && limit <= 500) ? limit : 100;

        try (StatelessSession s = DatabaseSessionFactory.createStatelessSession()) {
            CriteriaBuilder cb = s.getCriteriaBuilder();
            CriteriaQuery<UserHistory> q = cb.createQuery(UserHistory.class);
            Root<UserHistory> root = q.from(UserHistory.class);
            Predicate predicate = cb.equal(root.get("userId"), user.getId());
            if (before != null) {
                predicate = cb.and(predicate, cb.lessThan(root.get("watchedAt"), before));
            }
            q.select(root).where(predicate).orderBy(cb.desc(root.get("watchedAt")));
            List<UserHistory> items = s.createQuery(q).setMaxResults(max).list();
            return mapper.writeValueAsBytes(items);
        }
    }

    public static byte[] upsertHistoryResponse(String session, byte[] body) throws IOException {
        if (StringUtils.isBlank(session))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("session required"));

        User user = DatabaseHelper.getUserFromSession(session);
        if (user == null)
            ExceptionHandler.throwErrorResponse(new AuthenticationFailureResponse());

        JsonNode json = mapper.readTree(body);
        String videoId = json.has("videoId") ? json.get("videoId").asText() : null;
        if (StringUtils.isBlank(videoId))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("videoId required"));

        try (Session s = DatabaseSessionFactory.createSession()) {
            var tr = s.beginTransaction();
            CriteriaBuilder cb = s.getCriteriaBuilder();
            CriteriaQuery<UserHistory> q = cb.createQuery(UserHistory.class);
            Root<UserHistory> root = q.from(UserHistory.class);
            q.select(root).where(cb.and(
                cb.equal(root.get("userId"), user.getId()),
                cb.equal(root.get("videoId"), videoId)
            ));
            UserHistory existing = s.createQuery(q).uniqueResult();

            UserHistory entry = existing != null ? existing : new UserHistory();
            entry.setUserId(user.getId());
            entry.setVideoId(videoId);
            entry.setTitle(StringUtils.abbreviate(json.path("title").asText(""), 500));
            entry.setUploader(StringUtils.abbreviate(json.path("uploader").asText(""), 100));
            entry.setUploaderUrl(StringUtils.abbreviate(json.path("uploaderUrl").asText(""), 100));
            entry.setThumbnail(StringUtils.abbreviate(json.path("thumbnail").asText(""), 500));
            entry.setDuration(json.path("duration").asInt(0));
            entry.setPositionSeconds(json.path("positionSeconds").asDouble(0));
            entry.setWatchedAt(json.has("watchedAt") ? json.get("watchedAt").asLong() : System.currentTimeMillis());

            if (existing != null) s.merge(entry);
            else s.persist(entry);

            tr.commit();
            return mapper.writeValueAsBytes(new AcceptedResponse());
        }
    }

    public static byte[] deleteHistoryResponse(String session, String videoId) throws IOException {
        if (StringUtils.isBlank(session))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("session required"));

        User user = DatabaseHelper.getUserFromSession(session);
        if (user == null)
            ExceptionHandler.throwErrorResponse(new AuthenticationFailureResponse());

        try (Session s = DatabaseSessionFactory.createSession()) {
            var tr = s.beginTransaction();
            if (StringUtils.isBlank(videoId)) {
                s.createMutationQuery("DELETE FROM UserHistory WHERE userId = :uid")
                    .setParameter("uid", user.getId())
                    .executeUpdate();
            } else {
                s.createMutationQuery("DELETE FROM UserHistory WHERE userId = :uid AND videoId = :vid")
                    .setParameter("uid", user.getId())
                    .setParameter("vid", videoId)
                    .executeUpdate();
            }
            tr.commit();
            return mapper.writeValueAsBytes(new AcceptedResponse());
        }
    }
}
