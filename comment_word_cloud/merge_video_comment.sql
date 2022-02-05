select b.video_id,b.category,a.created_at,a.content 
from nico_data.comment_sample a
join nico_data.video_sample b
on a.video_id = b.video_id
